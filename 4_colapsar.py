# =============================================================================
# 4_colapsar.py
# Agrupa proposições apensadas em grupos, elege um representante por grupo
# e funde os campos segundo as seguintes regras:
#
# REGRAS DE FUSÃO:
#   - Representante: proposição principal interna ao grupo → mais antiga
#                    → menor ID (critério de desempate arbitrário)
#   - Situação do grupo: hierarquia Norma > Arquivada > Em tramitação.
#                        Os campos ultimoStatus_* vêm da proposição que
#                        determinou a situação do grupo.
#   - Temas e autores: união dos conjuntos de todas as apensadas (sem duplicatas)
#   - Demais campos escalares: first_non_null (representante tem prioridade)
#   - Campos com valores distintos entre apensadas: registrados em __valores_distintos
#
# Uso:
#   python 4_colapsar.py
# =============================================================================

import ast
import json
import logging
import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    INTERIM_DIR,
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
INPUT_PATH  = INTERIM_DIR / "proposicoes_enriquecidas.parquet"
OUTPUT_PATH = INTERIM_DIR / "proposicoes_colapsadas.parquet"

# Campos cujos conflitos entre apensadas são registrados em __valores_distintos
CAMPOS_AUDITORIA = ["uri", "siglaTipo", "numero", "ano", "codTipo"]

# Campos ultimoStatus_* que vêm da proposição que determinou a situação
CAMPOS_ULTIMO_STATUS = [
    "ultimoStatus_dataHora",
    "ultimoStatus_sequencia",
    "ultimoStatus_uriRelator",
    "ultimoStatus_idOrgao",
    "ultimoStatus_siglaOrgao",
    "ultimoStatus_uriOrgao",
    "ultimoStatus_regime",
    "ultimoStatus_descricaoTramitacao",
    "ultimoStatus_idTipoTramitacao",
    "ultimoStatus_descricaoSituacao",
    "ultimoStatus_idSituacao",
    "ultimoStatus_despacho",
    "ultimoStatus_apreciacao",
    "ultimoStatus_url",
]

# -----------------------------------------------------------------------------
# HELPERS DE PARSING
# -----------------------------------------------------------------------------

def parse_list(x) -> list:
    """Converte qualquer representação de lista para list Python."""
    if isinstance(x, list):
        return x
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
            try:
                v = ast.literal_eval(s)
                return v if isinstance(v, list) else [v]
            except Exception:
                return []
    return []


def extract_id_from_uri(uri) -> int | None:
    """Extrai ID numérico do final de uma URI da Câmara."""
    if pd.isna(uri) or uri is None:
        return None
    try:
        return int(str(uri).rstrip("/").split("/")[-1])
    except Exception:
        return None


def first_non_null(series: pd.Series):
    """Retorna o primeiro valor não-nulo de uma série."""
    for x in series:
        if isinstance(x, list):
            if len(x) > 0:
                return x
        elif pd.notna(x) and str(x).strip() not in ("", "nan", "None"):
            return x
    return np.nan


def unique_preserve_order(seq: list) -> list:
    """Remove duplicatas preservando ordem."""
    seen = set()
    out = []
    for x in seq:
        key = json.dumps(x, ensure_ascii=False, sort_keys=True) if isinstance(x, (dict, list)) else str(x)
        if key not in seen:
            seen.add(key)
            out.append(x)
    return out


# -----------------------------------------------------------------------------
# CLASSIFICAÇÃO DE SITUAÇÃO
# -----------------------------------------------------------------------------

def classificar_situacao(cod) -> str:
    """Classifica um código de situação nas 3 classes."""
    try:
        cod = int(float(cod))
    except Exception:
        return SITUACAO_ARQUIVADA

    if cod in C_NORMA:
        return SITUACAO_NORMA
    if cod in C_TRAMITACAO:
        return SITUACAO_TRAMITACAO
    if cod in C_ARQUIVADA or cod in C_ARQUIVADA_EXTRA:
        return SITUACAO_ARQUIVADA
    return SITUACAO_ARQUIVADA   # indefinido → arquivada


PRIORIDADE_SITUACAO = {
    SITUACAO_NORMA:      0,
    SITUACAO_ARQUIVADA:  1,
    SITUACAO_TRAMITACAO: 2,
}


# -----------------------------------------------------------------------------
# ELEIÇÃO DO REPRESENTANTE
# -----------------------------------------------------------------------------

def eleger_representante(grupo: pd.DataFrame) -> int:
    """
    Elege o índice (posição no DataFrame original) do representante do grupo.
    Prioridade:
      1. Proposição principal interna ao grupo (uriPropPrincipal aponta para membro do grupo)
      2. Mais antiga (dataApresentacao)
      3. Menor ID (desempate arbitrário)
    """
    ids_grupo = set(grupo["id"].tolist())

    # 1. procura proposição principal interna
    for idx, row in grupo.iterrows():
        principal_id = extract_id_from_uri(row.get("uriPropPrincipal"))
        if principal_id is not None and principal_id in ids_grupo:
            return idx

    # 2. mais antiga
    datas = pd.to_datetime(grupo["dataApresentacao"], errors="coerce")
    if datas.notna().any():
        return datas.idxmin()

    # 3. menor ID
    return grupo["id"].idxmin()


# -----------------------------------------------------------------------------
# FUSÃO DE UM GRUPO
# -----------------------------------------------------------------------------

def fundir_grupo(grupo: pd.DataFrame, rep_idx: int) -> dict:
    """
    Funde todas as proposições de um grupo em um único registro.
    """
    rep = grupo.loc[rep_idx]
    todas_colunas = grupo.columns.tolist()

    resultado = {}

    # --- IDs agrupados ---
    resultado["ids_agrupados"]    = sorted(grupo["id"].dropna().astype(int).tolist())
    resultado["n_ids_agrupados"]  = len(resultado["ids_agrupados"])
    resultado["id_representante"] = int(rep["id"])
    resultado["group_id"]         = int(rep["id"])   # group_id = id do representante

    # --- Situação do grupo (hierarquia Norma > Arquivada > Em tramitação) ---
    situacoes = grupo["ultimoStatus_idSituacao"].apply(classificar_situacao)
    situacao_grupo = min(situacoes, key=lambda s: PRIORIDADE_SITUACAO.get(s, 99))
    resultado["situacao_simplificada"] = situacao_grupo

    # proposição que determinou a situação
    det_mask = situacoes == situacao_grupo
    det_row  = grupo[det_mask].sort_values("ultimoStatus_dataHora", ascending=False).iloc[0]

    # campos ultimoStatus_* vêm da proposição determinante
    for col in CAMPOS_ULTIMO_STATUS:
        if col in grupo.columns:
            resultado[col] = det_row.get(col, np.nan)

    # --- Temas: união dos conjuntos ---
    todos_temas = []
    for x in grupo["temas_tuplas_json"]:
        todos_temas.extend(parse_list(x))
    # deduplicar por nome do tema (primeiro elemento da tupla)
    vistos_temas = set()
    temas_unicos = []
    for t in todos_temas:
        if isinstance(t, list) and len(t) >= 1:
            nome = str(t[0]).strip()
            if nome and nome not in vistos_temas:
                vistos_temas.add(nome)
                temas_unicos.append(t)
    resultado["temas_tuplas_json"] = json.dumps(temas_unicos, ensure_ascii=False)

    # temas_full_json: mantém o do representante
    resultado["temas_full_json"] = rep.get("temas_full_json", None)

    # --- Autores: união dos conjuntos ---
    todos_autores = []
    for x in grupo["autores_json"]:
        todos_autores.extend(parse_list(x))
    # deduplicar por uriAutor; fallback por nomeAutor
    vistos_autores = set()
    autores_unicos = []
    for a in todos_autores:
        if not isinstance(a, dict):
            continue
        chave = a.get("uriAutor") or a.get("nomeAutor")
        if chave and chave not in vistos_autores:
            vistos_autores.add(chave)
            autores_unicos.append(a)
    resultado["autores_json"] = json.dumps(autores_unicos, ensure_ascii=False)

    # autores só deputados (subset de autores_json)
    autores_deputados = [a for a in autores_unicos if a.get("codTipo") == 10000]
    resultado["autores_deputados"] = json.dumps(autores_deputados, ensure_ascii=False)

    # --- Campos escalares: first_non_null (representante tem prioridade) ---
    # reordena o grupo para que o representante venha primeiro
    grupo_reord = pd.concat([grupo.loc[[rep_idx]], grupo.drop(index=rep_idx)])

    campos_escalares = [
        c for c in todas_colunas
        if c not in (
            CAMPOS_ULTIMO_STATUS
            + ["id", "ultimoStatus_idSituacao",
               "temas_tuplas_json", "temas_full_json",
               "autores_json", "autores_deputados",
               "ids_agrupados", "n_ids_agrupados",
               "id_representante", "group_id",
               "situacao_simplificada", "ano_arquivo",
               "temas_ok", "temas_http", "temas_err",
               "autores_ok", "autores_err"]
        )
    ]

    for col in campos_escalares:
        resultado[col] = first_non_null(grupo_reord[col])

    # --- Auditoria: registra valores distintos ---
    for col in CAMPOS_AUDITORIA:
        if col not in grupo.columns:
            continue
        valores = grupo[col].dropna().unique().tolist()
        if len(valores) > 1:
            resultado[f"{col}__valores_distintos"] = json.dumps(valores, ensure_ascii=False)
        else:
            resultado[f"{col}__valores_distintos"] = np.nan

    # --- Metadados do enriquecimento ---
    resultado["temas_ok"]   = grupo["temas_ok"].any()   if "temas_ok"   in grupo.columns else False
    resultado["autores_ok"] = grupo["autores_ok"].any() if "autores_ok" in grupo.columns else False

    return resultado


# -----------------------------------------------------------------------------
# CONSTRUÇÃO DOS GRUPOS
# -----------------------------------------------------------------------------

def construir_grupos(df: pd.DataFrame) -> list[list[int]]:
    """
    Constrói grupos de proposições apensadas usando uriPropPrincipal.
    Retorna lista de grupos, cada grupo sendo uma lista de índices do DataFrame.

    Algoritmo Union-Find para lidar com cadeias de apensamento:
      A aponta para B, B aponta para C → grupo {A, B, C}
    """
    # mapa id → índice no df
    id_to_idx = {int(row["id"]): idx for idx, row in df.iterrows()}

    parent = {idx: idx for idx in df.index}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # une cada proposição com sua principal
    for idx, row in df.iterrows():
        principal_id = extract_id_from_uri(row.get("uriPropPrincipal"))
        if principal_id is not None and principal_id in id_to_idx:
            union(idx, id_to_idx[principal_id])

    # agrupa por raiz
    from collections import defaultdict
    grupos_dict = defaultdict(list)
    for idx in df.index:
        grupos_dict[find(idx)].append(idx)

    return list(grupos_dict.values())


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    if not INPUT_PATH.exists():
        log.error(f"Arquivo não encontrado: {INPUT_PATH}. Rode 2_enriquecer.py primeiro.")
        return

    log.info(f"Carregando {INPUT_PATH}...")
    df = pd.read_parquet(INPUT_PATH)
    log.info(f"Total de proposições: {len(df)}")

    # garante id inteiro
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"]).copy()
    df["id"] = df["id"].astype(int)
    df = df.reset_index(drop=True)

    # remove duplicatas de id
    antes = len(df)
    df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)
    if len(df) < antes:
        log.info(f"Removidas {antes - len(df)} duplicatas de ID.")

    # constrói grupos
    log.info("Construindo grupos de apensadas...")
    grupos = construir_grupos(df)

    n_singles = sum(1 for g in grupos if len(g) == 1)
    n_multi   = sum(1 for g in grupos if len(g) > 1)
    log.info(f"Grupos totais: {len(grupos)} ({n_singles} individuais, {n_multi} com apensadas)")

    # funde cada grupo
    log.info("Fundindo grupos...")
    registros = []
    for grupo_idxs in grupos:
        grupo_df = df.loc[grupo_idxs].copy()
        rep_idx  = eleger_representante(grupo_df)
        registro = fundir_grupo(grupo_df, rep_idx)
        registros.append(registro)

    df_colapsado = pd.DataFrame(registros)
    log.info(f"Proposições após colapso: {len(df_colapsado)}")
    log.info(f"Redução: {len(df)} → {len(df_colapsado)} ({(1 - len(df_colapsado)/len(df))*100:.1f}% de redução)")

    # distribuição de situação
    log.info("Distribuição de situação:")
    for sit, n in df_colapsado["situacao_simplificada"].value_counts().items():
        log.info(f"  {sit}: {n} ({n/len(df_colapsado)*100:.1f}%)")

    df_colapsado.to_parquet(OUTPUT_PATH, index=False)
    log.info(f"Salvo em: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
