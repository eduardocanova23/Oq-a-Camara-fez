# =============================================================================
# 2_enriquecer.py
# Para cada proposição coletada em data/raw/, busca via API da Câmara:
#   - Temas:   /proposicoes/{id}/temas
#   - Autores: /proposicoes/{id}/autores (todos os tipos de autor)
#
# Checkpoint: salva parquet parcial em data/interim/ a cada N proposições.
# Se o script for interrompido, retoma de onde parou na próxima execução.
#
# Uso:
#   python 2_enriquecer.py
# =============================================================================

import json
import time
import logging
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from config import (
    BASE_API,
    RAW_DIR,
    INTERIM_DIR,
    TIMEOUT,
    SLEEP_ENTRE_REQS,
    MAX_RETRIES,
)

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# CONFIG LOCAL
# -----------------------------------------------------------------------------
CHECKPOINT_PATH  = INTERIM_DIR / "proposicoes_enriquecidas.parquet"
CHECKPOINT_EVERY = 250   # salva parquet parcial a cada N proposições processadas

# -----------------------------------------------------------------------------
# HELPERS DE API
# -----------------------------------------------------------------------------

def get_json(session: requests.Session, url: str) -> list | dict | None:
    """
    Faz GET com retry e backoff exponencial.
    Retorna o campo 'dados' da resposta ou None em caso de falha definitiva.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            if resp.status_code == 404:
                return []   # proposição existe mas endpoint não tem dados
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limit (429) em {url}. Aguardando {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("dados", [])
        except Exception as e:
            last_err = e
            time.sleep(SLEEP_ENTRE_REQS * (2 ** attempt))

    log.warning(f"Falha definitiva em {url}: {last_err}")
    return None


def fetch_temas(session: requests.Session, prop_id: int) -> tuple[str | None, str | None, bool, int | None, str | None]:
    """
    Busca temas de uma proposição.
    Retorna: (temas_tuplas_json, temas_full_json, ok, http_status, erro)
    """
    url = f"{BASE_API}/proposicoes/{prop_id}/temas"
    try:
        resp = session.get(url, timeout=TIMEOUT)
        http = resp.status_code

        if resp.status_code == 404:
            return "[]", "[]", True, http, None

        if resp.status_code == 429:
            time.sleep(2)
            resp = session.get(url, timeout=TIMEOUT)
            http = resp.status_code

        resp.raise_for_status()
        dados = resp.json().get("dados", [])

        # formato tupla: [["Nome", relevancia], ...]
        tuplas = [[t.get("tema", ""), t.get("relevancia", 0)] for t in dados]
        temas_tuplas = json.dumps(tuplas, ensure_ascii=False)
        temas_full   = json.dumps(dados,  ensure_ascii=False)

        return temas_tuplas, temas_full, True, http, None

    except Exception as e:
        return None, None, False, None, str(e)


def fetch_autores(session: requests.Session, prop_id: int) -> tuple[str | None, bool, str | None]:
    """
    Busca autores de uma proposição (todos os tipos).
    Retorna: (autores_json, ok, erro)
    """
    url = f"{BASE_API}/proposicoes/{prop_id}/autores"
    try:
        resp = session.get(url, timeout=TIMEOUT)

        if resp.status_code == 404:
            return "[]", True, None

        if resp.status_code == 429:
            time.sleep(2)
            resp = session.get(url, timeout=TIMEOUT)

        resp.raise_for_status()
        dados = resp.json().get("dados", [])

        # normaliza cada autor para um dict consistente
        autores = []
        for a in dados:
            autores.append({
                "nomeAutor":       a.get("nome"),
                "uriAutor":        a.get("uri"),
                "codTipo":         a.get("codTipo"),
                "tipo":            a.get("tipo"),
                "ordemAssinatura": a.get("ordemAssinatura"),
                "proponente":      a.get("proponente"),
                # idDeputado só existe para autores do tipo deputado
                "idDeputado": _extract_id_from_uri(a.get("uri")) if a.get("codTipo") == 10000 else None,
            })

        return json.dumps(autores, ensure_ascii=False), True, None

    except Exception as e:
        return None, False, str(e)


def _extract_id_from_uri(uri: str | None) -> int | None:
    """Extrai o ID numérico do final de uma URI da Câmara."""
    if not uri:
        return None
    try:
        return int(str(uri).rstrip("/").split("/")[-1])
    except Exception:
        return None


# -----------------------------------------------------------------------------
# CHECKPOINT
# -----------------------------------------------------------------------------

def carregar_checkpoint() -> set[int]:
    """Retorna conjunto de IDs já processados no checkpoint parcial."""
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        df = pd.read_parquet(CHECKPOINT_PATH, columns=["id"])
        ids = set(df["id"].dropna().astype(int).tolist())
        log.info(f"Checkpoint encontrado: {len(ids)} proposições já processadas.")
        return ids
    except Exception as e:
        log.warning(f"Não foi possível ler o checkpoint: {e}. Começando do zero.")
        return set()


def salvar_checkpoint(df: pd.DataFrame) -> None:
    """Salva parquet parcial com as proposições processadas até agora."""
    try:
        df.to_parquet(CHECKPOINT_PATH, index=False)
    except Exception as e:
        log.error(f"Falha ao salvar checkpoint: {e}")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    # --- carrega todos os parquets do raw em um único DataFrame ---
    raw_files = sorted(RAW_DIR.glob("*.parquet"))
    if not raw_files:
        log.error(f"Nenhum arquivo encontrado em {RAW_DIR}. Rode 1_coletar.py primeiro.")
        return

    log.info(f"Carregando {len(raw_files)} arquivo(s) de {RAW_DIR}...")
    df_base = pd.concat(
        [pd.read_parquet(f) for f in raw_files],
        ignore_index=True,
    )
    log.info(f"Total de proposições: {len(df_base)}")

    # garante que id é inteiro
    df_base["id"] = pd.to_numeric(df_base["id"], errors="coerce")
    df_base = df_base.dropna(subset=["id"]).copy()
    df_base["id"] = df_base["id"].astype(int)

    # remove duplicatas (uma proposição pode aparecer em mais de um CSV anual)
    antes = len(df_base)
    df_base = df_base.drop_duplicates(subset=["id"]).copy()
    if len(df_base) < antes:
        log.info(f"Removidas {antes - len(df_base)} duplicatas de ID.")

    # --- inicializa colunas de enriquecimento ---
    for col, default in [
        ("temas_tuplas_json", None),
        ("temas_full_json",   None),
        ("temas_ok",          False),
        ("temas_http",        pd.NA),
        ("temas_err",         None),
        ("autores_json",      None),
        ("autores_ok",        False),
        ("autores_err",       None),
    ]:
        if col not in df_base.columns:
            df_base[col] = default

    # --- checkpoint: pula IDs já processados ---
    ids_prontos = carregar_checkpoint()

    if ids_prontos:
        # carrega o checkpoint e mescla com a base atual
        df_checkpoint = pd.read_parquet(CHECKPOINT_PATH)
        df_checkpoint["id"] = pd.to_numeric(df_checkpoint["id"], errors="coerce").astype(int)

        # atualiza as colunas de enriquecimento para os IDs já processados
        df_base = df_base.set_index("id")
        df_checkpoint = df_checkpoint.set_index("id")

        cols_enrich = [
            "temas_tuplas_json", "temas_full_json", "temas_ok",
            "temas_http", "temas_err",
            "autores_json", "autores_ok", "autores_err",
        ]
        for col in cols_enrich:
            if col in df_checkpoint.columns:
                df_base.loc[df_checkpoint.index, col] = df_checkpoint[col]

        df_base = df_base.reset_index()

    mask_pendente = ~df_base["id"].isin(ids_prontos)
    df_pendente   = df_base[mask_pendente].copy()

    log.info(f"Pendentes: {len(df_pendente)} | Já processadas: {len(ids_prontos)}")

    if df_pendente.empty:
        log.info("Nenhuma proposição pendente. Enriquecimento já concluído.")
        return

    # --- sessão HTTP ---
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    # --- loop principal ---
    processadas = 0
    erros_temas  = 0
    erros_autores = 0

    pbar = tqdm(df_pendente.itertuples(index=True), total=len(df_pendente), desc="Enriquecendo", unit="prop")

    for row in pbar:
        idx     = row.Index
        prop_id = int(row.id)

        # temas
        t_tuplas, t_full, t_ok, t_http, t_err = fetch_temas(session, prop_id)
        df_base.at[idx, "temas_tuplas_json"] = t_tuplas
        df_base.at[idx, "temas_full_json"]   = t_full
        df_base.at[idx, "temas_ok"]          = t_ok
        df_base.at[idx, "temas_http"]        = t_http
        df_base.at[idx, "temas_err"]         = t_err
        if not t_ok:
            erros_temas += 1

        time.sleep(SLEEP_ENTRE_REQS)

        # autores
        a_json, a_ok, a_err = fetch_autores(session, prop_id)
        df_base.at[idx, "autores_json"] = a_json
        df_base.at[idx, "autores_ok"]   = a_ok
        df_base.at[idx, "autores_err"]  = a_err
        if not a_ok:
            erros_autores += 1

        time.sleep(SLEEP_ENTRE_REQS)

        processadas += 1

        # checkpoint parcial
        if processadas % CHECKPOINT_EVERY == 0:
            salvar_checkpoint(df_base)
            pbar.set_postfix({
                "erros_temas":   erros_temas,
                "erros_autores": erros_autores,
                "checkpoint":    "salvo",
            })

    # checkpoint final
    salvar_checkpoint(df_base)

    log.info("=" * 60)
    log.info("Enriquecimento concluído.")
    log.info(f"Processadas nesta rodada: {processadas}")
    log.info(f"Erros em temas:   {erros_temas}")
    log.info(f"Erros em autores: {erros_autores}")
    log.info(f"Arquivo salvo em: {CHECKPOINT_PATH}")


if __name__ == "__main__":
    main()
