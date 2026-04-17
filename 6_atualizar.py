# =============================================================================
# 6_atualizar.py
# Orquestra o pipeline completo com lógica de delta:
#
#   1. Baixa todos os CSVs anuais (rápido — arquivos bulk)
#   2. Compara com a base atual usando ultimoStatus_dataHora
#   3. Enriquece via API APENAS as proposições novas ou modificadas (assíncrono)
#   4. Mescla com os dados anteriores das proposições não modificadas
#   5. Colapse e consolida
#
# Resultado: atualização completa em minutos em vez de horas.
#
# Uso:
#   python 6_atualizar.py
#
# Agendamento (exemplo cron — todo domingo às 3h):
#   0 3 * * 0 cd /caminho/do/projeto && python 6_atualizar.py >> logs/atualizar.log 2>&1
# =============================================================================

import io
import json
import time
import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

from config import (
    DATA_INICIO,
    DATA_FIM,
    TIPOS_RELEVANTES,
    BASE_API,
    BASE_ARQ,
    RAW_DIR,
    INTERIM_DIR,
    FINAL_DIR,
    STATE_FILE,
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
CONCURRENT_REQUESTS = 15    # requisições simultâneas para a API da Câmara
CHECKPOINT_EVERY    = 500   # salva checkpoint a cada N proposições enriquecidas
INTERIM_ENRICH      = INTERIM_DIR / "proposicoes_enriquecidas.parquet"
INTERIM_COLAPSADO   = INTERIM_DIR / "proposicoes_colapsadas.parquet"
FINAL_BASE          = FINAL_DIR   / "base_legislativa.parquet"

# -----------------------------------------------------------------------------
# ETAPA 1: COLETA DOS CSVs ANUAIS
# -----------------------------------------------------------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def anos_no_intervalo(d1: date, d2: date) -> list[int]:
    return list(range(d1.year, d2.year + 1))

def download_csv_ano(ano: int) -> pd.DataFrame:
    import requests
    url = BASE_ARQ.format(ano=ano)
    log.info(f"  Baixando {url}")
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            for enc in ("utf-8", "latin-1"):
                try:
                    df = pd.read_csv(
                        io.StringIO(resp.content.decode(enc)),
                        sep=";", dtype=str,
                        keep_default_na=False, na_values=[],
                    )
                    return df
                except UnicodeDecodeError:
                    continue
        except Exception as e:
            time.sleep(SLEEP_ENTRE_REQS * (2 ** attempt))
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Falha ao baixar CSV de {ano}: {e}")
    return pd.DataFrame()

def coletar() -> pd.DataFrame:
    """Baixa todos os CSVs anuais e retorna DataFrame concatenado e filtrado."""
    d1 = parse_date(DATA_INICIO)
    d2 = date.today() if DATA_FIM is None else parse_date(DATA_FIM)
    anos = anos_no_intervalo(d1, d2)
    log.info(f"Coletando anos: {anos}")

    dfs = []
    for ano in anos:
        try:
            df_ano = download_csv_ano(ano)
            dt = pd.to_datetime(df_ano["dataApresentacao"], errors="coerce")
            df_ano = df_ano[(dt.dt.date >= d1) & (dt.dt.date <= d2)].copy()
            df_ano = df_ano[df_ano["siglaTipo"].isin(TIPOS_RELEVANTES)].copy()
            df_ano.insert(0, "ano_arquivo", str(ano))
            dfs.append(df_ano)
            log.info(f"  Ano {ano}: {len(df_ano)} proposições")
        except Exception as e:
            log.error(f"  Erro no ano {ano}: {e}")

    df = pd.concat(dfs, ignore_index=True)
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"]).drop_duplicates(subset=["id"]).copy()
    df["id"] = df["id"].astype(int)
    log.info(f"Total coletado: {len(df)} proposições")
    return df

# -----------------------------------------------------------------------------
# ETAPA 2: IDENTIFICAÇÃO DO DELTA
# -----------------------------------------------------------------------------

def identificar_delta(df_novo: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compara o CSV novo com a base enriquecida atual.
    Retorna:
        df_novas       — IDs que não existiam antes
        df_modificadas — IDs que existiam mas tiveram ultimoStatus_dataHora alterado
        df_inalteradas — IDs sem mudança (não precisam ir para a API)
    """
    if not INTERIM_ENRICH.exists():
        log.info("Nenhuma base anterior encontrada. Tudo será enriquecido.")
        return df_novo, pd.DataFrame(), pd.DataFrame()

    df_base = pd.read_parquet(INTERIM_ENRICH, columns=["id", "ultimoStatus_dataHora"])
    df_base["id"] = pd.to_numeric(df_base["id"], errors="coerce").astype(int)
    df_base["_dt_base"] = pd.to_datetime(df_base["ultimoStatus_dataHora"], errors="coerce")
    df_base = df_base[["id", "_dt_base"]].copy()

    df_novo = df_novo.copy()
    df_novo["id"] = pd.to_numeric(df_novo["id"], errors="coerce").astype(int)
    df_novo["_dt_novo"] = pd.to_datetime(df_novo["ultimoStatus_dataHora"], errors="coerce")

    # mescla para comparar datas
    merged = df_novo.merge(df_base, on="id", how="left")

    # novas: ID não estava na base anterior (_dt_base é NaT)
    mask_nova = merged["_dt_base"].isna()

    # modificadas: ID existia mas data mudou
    mask_mod = ~mask_nova & (merged["_dt_novo"] != merged["_dt_base"])

    # inalteradas
    mask_inal = ~mask_nova & ~mask_mod

    # usa os IDs para filtrar o df_novo original (evita problema de índice)
    ids_novas      = merged.loc[mask_nova, "id"].tolist()
    ids_modificadas = merged.loc[mask_mod,  "id"].tolist()
    ids_inalteradas = merged.loc[mask_inal, "id"].tolist()

    df_novas       = df_novo[df_novo["id"].isin(ids_novas)].copy()
    df_modificadas = df_novo[df_novo["id"].isin(ids_modificadas)].copy()
    df_inalteradas = df_novo[df_novo["id"].isin(ids_inalteradas)].copy()

    # limpa colunas auxiliares
    for df in [df_novas, df_modificadas, df_inalteradas]:
        df.drop(columns=["_dt_novo"], errors="ignore", inplace=True)

    log.info(f"Delta — novas: {len(df_novas)} | modificadas: {len(df_modificadas)} | inalteradas: {len(df_inalteradas)}")
    return df_novas, df_modificadas, df_inalteradas

# -----------------------------------------------------------------------------
# ETAPA 3: ENRIQUECIMENTO ASSÍNCRONO
# -----------------------------------------------------------------------------

async def fetch_json(client: httpx.AsyncClient, url: str) -> list:
    """Faz GET assíncrono com retry. Retorna campo 'dados' ou []."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.get(url, timeout=TIMEOUT)
            if resp.status_code == 404:
                return []
            if resp.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json().get("dados", [])
        except Exception:
            await asyncio.sleep(SLEEP_ENTRE_REQS * (2 ** attempt))
    return []

async def enriquecer_um(client: httpx.AsyncClient, prop_id: int) -> dict:
    """Busca temas e autores de uma proposição de forma assíncrona."""
    temas_url   = f"{BASE_API}/proposicoes/{prop_id}/temas"
    autores_url = f"{BASE_API}/proposicoes/{prop_id}/autores"

    temas_dados, autores_dados = await asyncio.gather(
        fetch_json(client, temas_url),
        fetch_json(client, autores_url),
    )

    # temas
    tuplas = [[t.get("tema", ""), t.get("relevancia", 0)] for t in temas_dados]
    temas_tuplas = json.dumps(tuplas, ensure_ascii=False)
    temas_full   = json.dumps(temas_dados, ensure_ascii=False)
    temas_ok     = True

    # autores
    def extract_id(uri):
        if not uri:
            return None
        try:
            return int(str(uri).rstrip("/").split("/")[-1])
        except Exception:
            return None

    autores = []
    for a in autores_dados:
        autores.append({
            "nomeAutor":       a.get("nome"),
            "uriAutor":        a.get("uri"),
            "codTipo":         a.get("codTipo"),
            "tipo":            a.get("tipo"),
            "ordemAssinatura": a.get("ordemAssinatura"),
            "proponente":      a.get("proponente"),
            "idDeputado":      extract_id(a.get("uri")) if a.get("codTipo") == 10000 else None,
        })
    autores_json = json.dumps(autores, ensure_ascii=False)
    autores_ok   = True

    return {
        "id":                prop_id,
        "temas_tuplas_json": temas_tuplas,
        "temas_full_json":   temas_full,
        "temas_ok":          temas_ok,
        "autores_json":      autores_json,
        "autores_ok":        autores_ok,
    }

async def enriquecer_lote(ids: list[int]) -> pd.DataFrame:
    """Enriquece uma lista de IDs de forma assíncrona com concorrência limitada."""
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    resultados = []

    async def enriquecer_com_semaforo(client, prop_id):
        async with sem:
            resultado = await enriquecer_um(client, prop_id)
            return resultado

    limites = httpx.Limits(max_connections=CONCURRENT_REQUESTS + 5)
    async with httpx.AsyncClient(limits=limites) as client:
        tarefas = [enriquecer_com_semaforo(client, pid) for pid in ids]

        total = len(tarefas)
        concluidas = 0
        for coro in asyncio.as_completed(tarefas):
            resultado = await coro
            resultados.append(resultado)
            concluidas += 1
            if concluidas % 500 == 0 or concluidas == total:
                log.info(f"  Enriquecidas: {concluidas}/{total}")

    return pd.DataFrame(resultados)

def enriquecer(df_para_enriquecer: pd.DataFrame) -> pd.DataFrame:
    """Wrapper síncrono para o enriquecimento assíncrono."""
    ids = df_para_enriquecer["id"].astype(int).tolist()
    log.info(f"Enriquecendo {len(ids)} proposições (assíncrono, {CONCURRENT_REQUESTS} simultâneas)...")
    df_enrich = asyncio.run(enriquecer_lote(ids))
    df_enrich["id"] = df_enrich["id"].astype(int)

    # mescla com os dados base
    df_resultado = df_para_enriquecer.merge(df_enrich, on="id", how="left")
    return df_resultado

# -----------------------------------------------------------------------------
# ETAPA 4: MESCLAGEM COM BASE ANTERIOR
# -----------------------------------------------------------------------------

def mesclar_com_base_anterior(
    df_novas: pd.DataFrame,
    df_modificadas: pd.DataFrame,
    df_inalteradas_ids: list[int],
) -> pd.DataFrame:
    """
    Combina:
      - Proposições novas (enriquecidas agora)
      - Proposições modificadas (re-enriquecidas agora)
      - Proposições inalteradas (mantidas da base anterior)
    """
    partes = []

    if not df_novas.empty:
        partes.append(df_novas)

    if not df_modificadas.empty:
        partes.append(df_modificadas)

    if df_inalteradas_ids and INTERIM_ENRICH.exists():
        df_base = pd.read_parquet(INTERIM_ENRICH)
        df_base["id"] = pd.to_numeric(df_base["id"], errors="coerce").astype(int)
        df_inal = df_base[df_base["id"].isin(df_inalteradas_ids)].copy()
        log.info(f"Mantidas da base anterior: {len(df_inal)}")
        partes.append(df_inal)

    df_final = pd.concat(partes, ignore_index=True)
    df_final = df_final.drop_duplicates(subset=["id"]).copy()
    log.info(f"Base enriquecida consolidada: {len(df_final)} proposições")
    return df_final

# -----------------------------------------------------------------------------
# ETAPAS 5 E 6: COLAPSO E CONSOLIDAÇÃO
# -----------------------------------------------------------------------------

def rodar_colapso_e_consolidacao():
    """Importa e executa os scripts de colapso e consolidação."""
    log.info("Rodando 4_colapsar.py...")
    import importlib.util

    for script_name in ["4_colapsar", "5_consolidar"]:
        spec = importlib.util.spec_from_file_location(
            script_name, Path(__file__).parent / f"{script_name}.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.main()
        log.info(f"{script_name}.py concluído.")

# -----------------------------------------------------------------------------
# STATE
# -----------------------------------------------------------------------------

def atualizar_state(n_novas: int, n_modificadas: int, n_inalteradas: int):
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    state["ultima_atualizacao"] = datetime.now().isoformat()
    state["ultima_rodada"] = {
        "novas":       n_novas,
        "modificadas": n_modificadas,
        "inalteradas": n_inalteradas,
    }
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"State atualizado: {STATE_FILE}")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    inicio = datetime.now()
    log.info("=" * 60)
    log.info("INICIANDO ATUALIZAÇÃO DO PIPELINE")
    log.info("=" * 60)

    # 1. coleta
    log.info("--- ETAPA 1: Coleta ---")
    df_novo = coletar()

    # 2. delta
    log.info("--- ETAPA 2: Identificação do delta ---")
    df_novas, df_modificadas, df_inalteradas = identificar_delta(df_novo)
    ids_inalteradas = df_inalteradas["id"].astype(int).tolist()

    # 3. enriquecimento (só novas + modificadas)
    log.info("--- ETAPA 3: Enriquecimento assíncrono ---")
    partes_enriquecidas = []

    df_para_enriquecer = pd.concat([df_novas, df_modificadas], ignore_index=True)
    if not df_para_enriquecer.empty:
        df_enriquecido = enriquecer(df_para_enriquecer)
        partes_enriquecidas.append(df_enriquecido)
    else:
        log.info("Nenhuma proposição nova ou modificada — pulando enriquecimento.")

    # 4. mescla com base anterior
    log.info("--- ETAPA 4: Mesclagem com base anterior ---")
    df_novas_enrich      = partes_enriquecidas[0] if partes_enriquecidas else pd.DataFrame()
    df_modificadas_enrich = pd.DataFrame()

    if not df_para_enriquecer.empty and not df_novas.empty and not df_modificadas.empty:
        n_novas = len(df_novas)
        df_novas_enrich       = df_enriquecido.iloc[:n_novas].copy()
        df_modificadas_enrich = df_enriquecido.iloc[n_novas:].copy()
    elif not df_para_enriquecer.empty:
        if not df_novas.empty:
            df_novas_enrich = df_enriquecido
        else:
            df_modificadas_enrich = df_enriquecido

    df_mesclado = mesclar_com_base_anterior(
        df_novas_enrich,
        df_modificadas_enrich,
        ids_inalteradas,
    )

    # salva base enriquecida atualizada
    df_mesclado.to_parquet(INTERIM_ENRICH, index=False)
    log.info(f"Base enriquecida salva: {INTERIM_ENRICH}")

    # 5 e 6. colapso e consolidação
    log.info("--- ETAPAS 5-6: Colapso e consolidação ---")
    rodar_colapso_e_consolidacao()

    # state
    atualizar_state(
        n_novas=len(df_novas),
        n_modificadas=len(df_modificadas),
        n_inalteradas=len(ids_inalteradas),
    )

    duracao = (datetime.now() - inicio).seconds
    log.info("=" * 60)
    log.info(f"ATUALIZAÇÃO CONCLUÍDA em {duracao // 60}min {duracao % 60}s")
    log.info(f"  Novas:       {len(df_novas)}")
    log.info(f"  Modificadas: {len(df_modificadas)}")
    log.info(f"  Inalteradas: {len(ids_inalteradas)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
