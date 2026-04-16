# =============================================================================
# 1_coletar.py
# Baixa os CSVs anuais de proposições da Câmara dos Deputados,
# filtra pelos tipos relevantes e salva um parquet por ano em data/raw/.
#
# Comportamento:
#   - Re-baixa TODOS os anos a cada rodada (sem lógica de delta aqui).
#   - Isso garante que mudanças de situação em proposições antigas sejam capturadas.
#   - Otimização futura: delta por ultimoStatus_dataHora (ver ARQUITETURA.docx §6.1).
#
# Uso:
#   python 1_coletar.py
# =============================================================================

import io
import time
import json
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests

from config import (
    DATA_INICIO,
    DATA_FIM,
    TIPOS_RELEVANTES,
    BASE_ARQ,
    RAW_DIR,
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
# HELPERS
# -----------------------------------------------------------------------------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def anos_no_intervalo(d1: date, d2: date) -> list[int]:
    return list(range(d1.year, d2.year + 1))


def download_csv_ano(ano: int, session: requests.Session) -> pd.DataFrame:
    """
    Baixa o CSV anual do endpoint /arquivos da Câmara.
    Tenta utf-8 primeiro, depois latin-1.
    Retorna DataFrame com todas as proposições do ano.
    """
    url = BASE_ARQ.format(ano=ano)
    log.info(f"Baixando {url}")

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limit (429). Aguardando {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()

            raw = resp.content
            for enc in ("utf-8", "latin-1"):
                try:
                    text = raw.decode(enc)
                    df = pd.read_csv(
                        io.StringIO(text),
                        sep=";",
                        dtype=str,
                        keep_default_na=False,
                        na_values=[],
                    )
                    log.info(f"  Ano {ano}: {len(df)} proposições (encoding: {enc})")
                    return df
                except UnicodeDecodeError:
                    continue

            raise RuntimeError(f"Falha de encoding no CSV do ano {ano}")

        except Exception as e:
            last_err = e
            wait = SLEEP_ENTRE_REQS * (2 ** attempt)
            log.warning(f"  Tentativa {attempt + 1}/{MAX_RETRIES} falhou: {e}. Aguardando {wait:.1f}s...")
            time.sleep(wait)

    raise RuntimeError(f"Não foi possível baixar o CSV do ano {ano} após {MAX_RETRIES} tentativas. Último erro: {last_err}")


def filtrar_periodo(df: pd.DataFrame, d1: date, d2: date) -> pd.DataFrame:
    """Remove proposições fora da janela temporal definida no config."""
    if "dataApresentacao" not in df.columns:
        raise KeyError("Coluna 'dataApresentacao' não encontrada no CSV.")

    dt = pd.to_datetime(df["dataApresentacao"], errors="coerce")
    mask = (dt.dt.date >= d1) & (dt.dt.date <= d2)
    return df.loc[mask].copy()


def filtrar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Mantém apenas os tipos de proposição definidos em TIPOS_RELEVANTES."""
    if "siglaTipo" not in df.columns:
        raise KeyError("Coluna 'siglaTipo' não encontrada no CSV.")

    antes = len(df)
    df = df[df["siglaTipo"].isin(TIPOS_RELEVANTES)].copy()
    log.info(f"  Filtro de tipos: {antes} → {len(df)} ({len(df)/antes*100:.1f}% mantidas)")
    return df


def salvar_parquet(df: pd.DataFrame, ano: int) -> Path:
    """Salva o DataFrame como parquet em data/raw/."""
    path = RAW_DIR / f"proposicoes_{ano}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"  Salvo: {path} ({len(df)} linhas)")
    return path


def atualizar_state(anos_processados: list[int]) -> None:
    """Registra timestamp e anos processados nesta rodada."""
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    state["ultima_coleta"] = datetime.now().isoformat()
    state["anos_coletados"] = sorted(anos_processados)

    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"State atualizado: {STATE_FILE}")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    d1 = parse_date(DATA_INICIO)
    d2 = date.today() if DATA_FIM is None else parse_date(DATA_FIM)

    log.info(f"Período: {d1} → {d2}")
    log.info(f"Tipos relevantes: {sorted(TIPOS_RELEVANTES)}")
    log.info(f"Destino: {RAW_DIR}")

    anos = anos_no_intervalo(d1, d2)
    log.info(f"Anos a coletar: {anos}")

    session = requests.Session()
    session.headers.update({"Accept": "text/csv,application/octet-stream"})

    anos_processados = []
    total_proposicoes = 0

    for ano in anos:
        log.info(f"--- Ano {ano} ---")
        try:
            df_ano = download_csv_ano(ano, session)

            # filtra pelo período exato (ex: 2019 pode ter proposições de dez/2018)
            df_ano = filtrar_periodo(df_ano, d1, d2)

            # filtra pelos tipos relevantes
            df_ano = filtrar_tipos(df_ano)

            if df_ano.empty:
                log.warning(f"  Ano {ano}: nenhuma proposição após filtros. Pulando.")
                continue

            # adiciona coluna de auditoria
            df_ano.insert(0, "ano_arquivo", str(ano))

            salvar_parquet(df_ano, ano)
            anos_processados.append(ano)
            total_proposicoes += len(df_ano)

        except Exception as e:
            log.error(f"  Erro ao processar ano {ano}: {e}")
            # não interrompe — continua tentando os outros anos
            continue

        time.sleep(SLEEP_ENTRE_REQS)

    atualizar_state(anos_processados)

    log.info("=" * 60)
    log.info(f"Coleta concluída.")
    log.info(f"Anos processados: {anos_processados}")
    log.info(f"Total de proposições coletadas: {total_proposicoes}")
    log.info(f"Arquivos em: {RAW_DIR}")


if __name__ == "__main__":
    main()
