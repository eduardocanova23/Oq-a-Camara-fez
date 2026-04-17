# =============================================================================
# 5_consolidar.py
# Recebe a base colapsada, faz a limpeza final das colunas,
# garante consistência dos tipos e salva o parquet final em data/final/.
#
# Também gera um resumo estatístico da base para auditoria.
#
# Uso:
#   python 5_consolidar.py
# =============================================================================

import json
import logging
import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    INTERIM_DIR,
    FINAL_DIR,
    SITUACAO_NORMA,
    SITUACAO_TRAMITACAO,
    SITUACAO_ARQUIVADA,
    C_NORMA,
    C_TRAMITACAO,
    C_ARQUIVADA,
    C_ARQUIVADA_EXTRA,
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
# CAMINHOS
# -----------------------------------------------------------------------------
INPUT_PATH  = INTERIM_DIR / "proposicoes_colapsadas.parquet"
OUTPUT_PATH = FINAL_DIR   / "base_legislativa.parquet"

# -----------------------------------------------------------------------------
# COLUNAS FINAIS
# Ordem e nomes das colunas na base final.
# Colunas intermediárias de controle (temas_ok, autores_ok, etc.) são descartadas.
# -----------------------------------------------------------------------------
COLUNAS_FINAIS = [
    # identidade do grupo
    "group_id",
    "id_representante",
    "ids_agrupados",
    "n_ids_agrupados",

    # dados básicos da proposição
    "siglaTipo",
    "descricaoTipo",
    "numero",
    "ano",
    "dataApresentacao",
    "ementa",
    "ementaDetalhada",
    "keywords",
    "uri",
    "urlInteiroTeor",

    # situação
    "situacao_simplificada",
    "ultimoStatus_idSituacao",
    "ultimoStatus_descricaoSituacao",
    "ultimoStatus_dataHora",
    "ultimoStatus_siglaOrgao",
    "ultimoStatus_descricaoTramitacao",
    "ultimoStatus_despacho",
    "ultimoStatus_regime",
    "ultimoStatus_apreciacao",
    "ultimoStatus_uriRelator",
    "ultimoStatus_idOrgao",
    "ultimoStatus_uriOrgao",
    "ultimoStatus_idTipoTramitacao",
    "ultimoStatus_sequencia",
    "ultimoStatus_url",

    # relações entre proposições
    "uriPropPrincipal",
    "uriPropAnterior",
    "uriPropPosterior",

    # temas e autores
    "temas_tuplas_json",
    "autores_json",
    "autores_deputados",

    # auditoria de conflitos entre apensadas
    "uri__valores_distintos",
    "siglaTipo__valores_distintos",
    "numero__valores_distintos",
    "ano__valores_distintos",
    "codTipo__valores_distintos",
]

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def garantir_coluna(df: pd.DataFrame, col: str, default=np.nan) -> pd.DataFrame:
    """Cria coluna com valor padrão se não existir."""
    if col not in df.columns:
        df[col] = default
    return df


def limpar_string(s) -> str | None:
    """Normaliza strings: strip, vazio → None."""
    if pd.isna(s) or s is None:
        return None
    s = str(s).strip()
    return s if s and s.lower() not in ("nan", "none", "null") else None


def parse_list_safe(x) -> list:
    """Converte campo de lista para list Python sem lançar exceção."""
    if isinstance(x, (list, np.ndarray)):
        return list(x)
    if x is None:
        return []
    try:
        if pd.isna(x):
            return []
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            return v if isinstance(v, list) else [v]
        except Exception:
            pass
    return []


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    if not INPUT_PATH.exists():
        log.error(f"Arquivo não encontrado: {INPUT_PATH}. Rode 4_colapsar.py primeiro.")
        return

    log.info(f"Carregando {INPUT_PATH}...")
    df = pd.read_parquet(INPUT_PATH)
    log.info(f"Total de grupos: {len(df)}")

    # --- garante que todas as colunas finais existem ---
    for col in COLUNAS_FINAIS:
        df = garantir_coluna(df, col)

    # --- seleciona e reordena colunas ---
    df = df[COLUNAS_FINAIS].copy()

    # --- normaliza tipos ---

    # inteiros
    for col in ["group_id", "id_representante", "n_ids_agrupados", "numero", "ano"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # datas
    df["dataApresentacao"]      = pd.to_datetime(df["dataApresentacao"],      errors="coerce")
    df["ultimoStatus_dataHora"] = pd.to_datetime(df["ultimoStatus_dataHora"], errors="coerce")

    # strings — normaliza campos de texto livre
    for col in ["ementa", "ementaDetalhada", "keywords",
                "ultimoStatus_descricaoSituacao", "ultimoStatus_descricaoTramitacao",
                "ultimoStatus_despacho", "ultimoStatus_regime", "ultimoStatus_apreciacao"]:
        if col in df.columns:
            df[col] = df[col].apply(limpar_string)

    # ids_agrupados — garante que é lista de ints serializada como JSON
    def normalizar_ids_agrupados(x):
        lst = parse_list_safe(x)
        return json.dumps([int(i) for i in lst], ensure_ascii=False)

    df["ids_agrupados"] = df["ids_agrupados"].apply(normalizar_ids_agrupados)

    # --- validação final ---
    n_sem_situacao = df["situacao_simplificada"].isna().sum()
    if n_sem_situacao > 0:
        log.warning(f"{n_sem_situacao} grupos sem situacao_simplificada — verificar.")

    n_sem_tipo = df["siglaTipo"].isna().sum()
    if n_sem_tipo > 0:
        log.warning(f"{n_sem_tipo} grupos sem siglaTipo.")

    # --- salva ---
    df.to_parquet(OUTPUT_PATH, index=False)
    log.info(f"Base final salva em: {OUTPUT_PATH}")

    # --- resumo estatístico ---
    log.info("=" * 60)
    log.info("RESUMO DA BASE FINAL")
    log.info(f"Total de grupos:       {len(df)}")
    log.info(f"Proposições cobertas:  {df['n_ids_agrupados'].sum()}")
    log.info(f"Período:               {df['dataApresentacao'].min().date()} → {df['dataApresentacao'].max().date()}")
    log.info("")
    log.info("Situação:")
    for sit, n in df["situacao_simplificada"].value_counts().items():
        log.info(f"  {sit}: {n} ({n/len(df)*100:.1f}%)")
    log.info("")
    log.info("Tipos:")
    for tipo, n in df["siglaTipo"].value_counts().items():
        log.info(f"  {tipo}: {n}")
    log.info("")
    log.info("Grupos com apensadas:")
    log.info(f"  {(df['n_ids_agrupados'] > 1).sum()} grupos com mais de 1 proposição")
    log.info(f"  Máximo de apensadas num grupo: {df['n_ids_agrupados'].max()}")
    log.info("")

    # temas
    todos_temas = []
    for x in df["temas_tuplas_json"]:
        for t in parse_list_safe(x):
            if isinstance(t, list) and len(t) >= 1:
                todos_temas.append(str(t[0]))
    sem_tema = df["temas_tuplas_json"].apply(lambda x: len(parse_list_safe(x)) == 0).sum()
    log.info(f"Grupos sem tema:       {sem_tema} ({sem_tema/len(df)*100:.1f}%)")

    from collections import Counter
    top_temas = Counter(todos_temas).most_common(5)
    log.info("Top 5 temas:")
    for tema, n in top_temas:
        log.info(f"  {tema}: {n}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
