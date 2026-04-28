# =============================================================================
# utils/dados.py
# Carrega e filtra os dados do pipeline para o dashboard.
# =============================================================================

import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------------
# CAMINHOS
# -----------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent.parent
_PIPELINE_FINAL = _ROOT / "pipeline" / "data" / "final"

PATH_BASE      = _PIPELINE_FINAL / "base_legislativa.parquet"
PATH_DEPUTADOS  = _PIPELINE_FINAL / "deputados.parquet"
PATH_COAUTORIAS = _PIPELINE_FINAL / "coautorias.parquet"

# -----------------------------------------------------------------------------
# CONSTANTES
# -----------------------------------------------------------------------------
SITUACOES = [
    "Em tramitação",
    "Arquivada",
    "Transformada em norma jurídica",
]

TIPOS = ["PL", "PEC", "PLP", "PDL", "PDC"]

COR_SITUACAO = {
    "Em tramitação":                 "#2E75B6",
    "Arquivada":                     "#A6A6A6",
    "Transformada em norma jurídica":"#70AD47",
}

MODOS_PARTIDO = {
    "Só o autor principal":        "principal",
    "Todos que tiveram coautoria": "todos",
}

# agrupamento de tipos de autor para o filtro
GRUPOS_AUTOR = {
    "Deputado(a)":      ["Deputado(a)"],
    "Comissão":         ["COMISSÃO PERMANENTE", "COMISSÃO MISTA PERMANENTE",
                         "COMISSÃO PARLAMENTAR DE INQUÉRITO", "COMISSÃO DIRETORA",
                         "COMISSÃO ESPECIAL", "MISTA CPI"],
    "Poder Executivo":  ["Órgão do Poder Executivo"],
    "Senado":           ["Senador(a)", "Órgão do Senado Federal", "PERMANENTE DO SENADO FEDERAL"],
    "Outros":           ["Órgão do Poder Legislativo", "Órgão do Poder Judiciário",
                         "Sociedade Civil", "MPU - Ministério Público da União",
                         "DPU - Defensoria Pública da União"],
}

# mapa inverso: tipo raw → grupo
_TIPO_PARA_GRUPO = {
    tipo: grupo
    for grupo, tipos in GRUPOS_AUTOR.items()
    for tipo in tipos
}

# -----------------------------------------------------------------------------
# HELPERS DE PARSING
# -----------------------------------------------------------------------------

def parse_list_safe(x) -> list:
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


def _parse_temas(x) -> list[str]:
    if pd.isna(x) or x is None:
        return []
    try:
        dados = json.loads(x) if isinstance(x, str) else x
        return [str(t[0]).strip() for t in dados if isinstance(t, list) and len(t) >= 1 and t[0]]
    except Exception:
        return []


def _extrair_autor_principal(x) -> dict | None:
    if pd.isna(x) or x is None:
        return None
    try:
        autores = json.loads(x) if isinstance(x, str) else x
        if not autores:
            return None
        autores_validos = [a for a in autores if isinstance(a, dict)]
        if not autores_validos:
            return None
        return min(autores_validos, key=lambda a: a.get("ordemAssinatura") or 999)
    except Exception:
        return None


def _calcular_dias_tramitacao(df: pd.DataFrame) -> pd.Series:
    hoje = pd.Timestamp.today().normalize()
    data_fim = df["ultimoStatus_dataHora"].copy()
    mask_tram = df["situacao_simplificada"] == "Em tramitação"
    data_fim[mask_tram] = hoje
    dias = (data_fim - df["dataApresentacao"]).dt.days
    return dias.clip(lower=0)


def _partidos_da_proposicao(autores_json, mapa_partido: dict) -> set[str]:
    """Retorna partidos de todos os coautores deputados de uma proposição."""
    autores = parse_list_safe(autores_json)
    partidos = set()
    for a in autores:
        if not isinstance(a, dict):
            continue
        id_dep = a.get("idDeputado")
        if id_dep is not None:
            try:
                partido = mapa_partido.get(int(id_dep))
                if partido:
                    partidos.add(partido)
            except (ValueError, TypeError):
                continue
    return partidos


def _grupos_autor_da_proposicao(autores_json) -> set[str]:
    """Retorna os grupos de tipo de autor presentes numa proposição."""
    autores = parse_list_safe(autores_json)
    grupos = set()
    for a in autores:
        if not isinstance(a, dict):
            continue
        tipo = a.get("tipo", "")
        grupo = _TIPO_PARA_GRUPO.get(tipo, "Outros")
        grupos.add(grupo)
    return grupos


# -----------------------------------------------------------------------------
# CARREGAMENTO
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def carregar_base() -> pd.DataFrame:
    df = pd.read_parquet(PATH_BASE)
    df["dataApresentacao"]      = pd.to_datetime(df["dataApresentacao"],      errors="coerce")
    df["ultimoStatus_dataHora"] = pd.to_datetime(df["ultimoStatus_dataHora"], errors="coerce")
    df["ano_apresentacao"] = df["dataApresentacao"].dt.year
    df["mes_apresentacao"] = df["dataApresentacao"].dt.to_period("M").dt.to_timestamp()
    df["_temas_lista"]     = df["temas_tuplas_json"].apply(_parse_temas)
    df["_autor_principal"] = df["autores_deputados"].apply(_extrair_autor_principal)
    df["_dias_tramitacao"] = _calcular_dias_tramitacao(df)
    df["_grupos_autor"]    = df["autores_json"].apply(_grupos_autor_da_proposicao)
    return df


@st.cache_data(ttl=3600)
def carregar_deputados() -> pd.DataFrame:
    df = pd.read_parquet(PATH_DEPUTADOS)
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    return df


@st.cache_data(ttl=3600)
def base_com_deputados() -> pd.DataFrame:
    df   = carregar_base().copy()
    deps = carregar_deputados()[["id", "siglaPartido", "siglaUf", "urlFoto", "federacao"]].copy()
    deps.columns = ["_id_autor", "partido_autor", "uf_autor", "foto_autor", "federacao_autor"]
    df["_id_autor"] = df["_autor_principal"].apply(
        lambda a: a.get("idDeputado") if isinstance(a, dict) else None
    )
    df["_id_autor"] = pd.to_numeric(df["_id_autor"], errors="coerce").astype("Int64")
    df = df.merge(deps, on="_id_autor", how="left")
    return df


# -----------------------------------------------------------------------------
# FILTROS
# -----------------------------------------------------------------------------

def filtrar(
    df: pd.DataFrame,
    data_inicio: pd.Timestamp | None = None,
    data_fim: pd.Timestamp | None = None,
    situacoes: list[str] | None = None,
    tipos: list[str] | None = None,
    temas: list[str] | None = None,
    partidos: list[str] | None = None,
    ufs: list[str] | None = None,
    modo_partido: str = "principal",
    grupos_autor: list[str] | None = None,
) -> pd.DataFrame:
    """
    Aplica filtros ao DataFrame.

    grupos_autor: lista de grupos de tipo de autor (ex: ["Deputado(a)", "Comissão"])
                  None = todos os tipos

    modo_partido (só relevante quando grupos_autor inclui "Deputado(a)"):
        "principal" — filtra pelo partido do autor principal
        "todos"     — inclui proposições onde qualquer coautor deputado é do partido
    """
    mask = pd.Series(True, index=df.index)

    if data_inicio:
        mask &= df["dataApresentacao"] >= pd.Timestamp(data_inicio)
    if data_fim:
        mask &= df["dataApresentacao"] <= pd.Timestamp(data_fim)
    if situacoes:
        mask &= df["situacao_simplificada"].isin(situacoes)
    if tipos:
        mask &= df["siglaTipo"].isin(tipos)
    if temas:
        mask &= df["_temas_lista"].apply(lambda lst: bool(set(lst) & set(temas)))

    # filtro de tipo de autor
    if grupos_autor:
        grupos_set = set(grupos_autor)
        mask &= df["_grupos_autor"].apply(lambda g: bool(g & grupos_set))

    # filtro de partido (só faz sentido com Deputado(a) selecionado)
    if partidos and (not grupos_autor or "Deputado(a)" in grupos_autor):
        partidos_set = set(partidos)
        if modo_partido == "todos":
            deps = carregar_deputados()[["id", "siglaPartido"]].dropna()
            mapa = dict(zip(deps["id"].astype(int), deps["siglaPartido"]))
            mask &= df["autores_deputados"].apply(
                lambda x: bool(_partidos_da_proposicao(x, mapa) & partidos_set)
            )
        else:
            if "partido_autor" in df.columns:
                mask &= df["partido_autor"].isin(partidos_set)

    if ufs and "uf_autor" in df.columns:
        mask &= df["uf_autor"].isin(ufs)

    return df[mask].copy()


# -----------------------------------------------------------------------------
# AGREGAÇÕES
# -----------------------------------------------------------------------------

def contagem_por_situacao(df: pd.DataFrame) -> pd.DataFrame:
    cont = (
        df["situacao_simplificada"]
        .value_counts()
        .rename_axis("situacao")
        .reset_index(name="n")
    )
    cont["pct"] = (cont["n"] / cont["n"].sum() * 100).round(1)
    return cont


def contagem_por_tema(df: pd.DataFrame) -> pd.DataFrame:
    exploded = df[["group_id", "_temas_lista", "situacao_simplificada"]].copy()
    exploded = exploded.explode("_temas_lista").rename(columns={"_temas_lista": "tema"})
    exploded = exploded[exploded["tema"].notna() & (exploded["tema"] != "")]
    cont = (
        exploded.groupby("tema")
        .agg(
            total=("group_id", "count"),
            normas=("situacao_simplificada", lambda x: (x == "Transformada em norma jurídica").sum()),
            arquivadas=("situacao_simplificada", lambda x: (x == "Arquivada").sum()),
            tramitando=("situacao_simplificada", lambda x: (x == "Em tramitação").sum()),
        )
        .reset_index()
    )
    cont["pct_norma"] = (cont["normas"] / cont["total"] * 100).round(1)
    return cont.sort_values("total", ascending=False).reset_index(drop=True)


def serie_temporal(df: pd.DataFrame, freq: str = "M") -> pd.DataFrame:
    df = df.copy()
    df["_periodo"] = df["dataApresentacao"].dt.year if freq == "Y" else df["mes_apresentacao"]
    return (
        df.groupby("_periodo")
        .size()
        .rename_axis("periodo")
        .reset_index(name="n")
        .sort_values("periodo")
    )


def tempo_tramitacao_por_tema(df: pd.DataFrame) -> pd.DataFrame:
    df_final = df[df["situacao_simplificada"].isin([
        "Transformada em norma jurídica", "Arquivada"
    ])].copy()
    exploded = df_final[["group_id", "_temas_lista", "_dias_tramitacao"]].copy()
    exploded = exploded.explode("_temas_lista").rename(columns={"_temas_lista": "tema"})
    exploded = exploded[exploded["tema"].notna() & (exploded["tema"] != "")]
    return (
        exploded.groupby("tema")["_dias_tramitacao"]
        .agg(["mean", "median", "count"])
        .rename(columns={"mean": "media_dias", "median": "mediana_dias", "count": "n"})
        .reset_index()
        .query("n >= 10")
        .sort_values("mediana_dias", ascending=False)
        .reset_index(drop=True)
    )


def lista_temas_disponiveis(df: pd.DataFrame) -> list[str]:
    todos = set()
    for lst in df["_temas_lista"]:
        todos.update(lst)
    return sorted(todos)


def lista_partidos_disponiveis(df: pd.DataFrame) -> list[str]:
    if "partido_autor" not in df.columns:
        return []
    return sorted(df["partido_autor"].dropna().unique().tolist())


def contar_deputados_envolvidos(
    df: pd.DataFrame,
    partidos: list[str] | None = None,
    modo_partido: str = "principal",
) -> int:
    """
    Conta deputados únicos nas proposições filtradas.

    - modo "principal" sem partido: conta autores principais únicos
    - modo "principal" com partido: conta autores principais do partido
    - modo "todos" sem partido: conta todos os coautores únicos
    - modo "todos" com partido: conta todos os coautores do partido
    """
    deps = carregar_deputados()[["id", "siglaPartido"]].dropna()
    mapa = dict(zip(deps["id"].astype(int), deps["siglaPartido"]))
    partidos_set = set(partidos) if partidos else None

    if modo_partido == "principal":
        ids = df["_autor_principal"].apply(
            lambda a: (
                int(a["idDeputado"])
                if isinstance(a, dict)
                and a.get("idDeputado") is not None
                and (partidos_set is None or mapa.get(int(a["idDeputado"]), "") in partidos_set)
                else None
            )
        ).dropna().nunique()
    else:
        ids = (
            df["autores_deputados"]
            .apply(lambda x: [
                int(a["idDeputado"]) for a in parse_list_safe(x)
                if isinstance(a, dict)
                and a.get("idDeputado") is not None
                and (partidos_set is None or mapa.get(int(a["idDeputado"]), "") in partidos_set)
            ])
            .explode()
            .dropna()
            .nunique()
        )
    return int(ids)


# -----------------------------------------------------------------------------
# DEPUTADOS — FUNÇÕES ESPECÍFICAS
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def carregar_coautorias() -> pd.DataFrame:
    """Carrega o parquet de coautorias pré-calculadas."""
    df = pd.read_parquet(PATH_COAUTORIAS)
    df["id_deputado"] = pd.to_numeric(df["id_deputado"], errors="coerce").astype(int)
    return df


def proposicoes_do_deputado(
    df_base: pd.DataFrame,
    dep_id: int,
    modo: str = "principal",
) -> pd.DataFrame:
    """
    Retorna proposições associadas a um deputado.

    modo:
        "principal" — só onde é autor principal
        "coautor"   — onde aparece como coautor mas não é o principal
        "ambos"     — os dois
    """
    # sempre calcula ambas as máscaras para evitar UnboundLocalError
    mask_principal = df_base["_autor_principal"].apply(
        lambda a: isinstance(a, dict) and int(a.get("idDeputado", 0)) == dep_id
    )
    mask_coautor = df_base["autores_deputados"].apply(
        lambda x: any(
            isinstance(a, dict) and int(a.get("idDeputado", 0)) == dep_id
            for a in parse_list_safe(x)
        )
    )

    if modo == "principal":
        return df_base[mask_principal].copy()
    elif modo == "coautor":
        return df_base[mask_coautor & ~mask_principal].copy()
    else:
        return df_base[mask_principal | mask_coautor].copy()


def top_coautores(dep_id: int, n: int = 5) -> list[dict]:
    """
    Retorna os top N coautores de um deputado com dados enriquecidos.
    Cada item: {id_coautor, n_proposicoes, nome, siglaPartido, siglaUf, urlFoto}
    """
    df_co = carregar_coautorias()
    row = df_co[df_co["id_deputado"] == dep_id]
    if row.empty:
        return []

    coautores_raw = json.loads(row.iloc[0]["coautores_json"])[:n]

    deps = carregar_deputados().set_index("id")

    resultado = []
    for item in coautores_raw:
        cid = item["id_coautor"]
        try:
            dep_info = deps.loc[cid]
            resultado.append({
                "id_coautor":    cid,
                "n_proposicoes": item["n_proposicoes"],
                "nome":          dep_info.get("nome", "—"),
                "siglaPartido":  dep_info.get("siglaPartido", "—"),
                "siglaUf":       dep_info.get("siglaUf", "—"),
                "urlFoto":       dep_info.get("urlFoto", None),
            })
        except KeyError:
            continue

    return resultado


def perfil_tematico(df_props: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna contagem de proposições por tema para um conjunto de proposições.
    Útil para o gráfico de pizza do perfil do deputado.
    """
    exploded = df_props[["group_id", "_temas_lista"]].copy()
    exploded = exploded.explode("_temas_lista").rename(columns={"_temas_lista": "tema"})
    exploded = exploded[exploded["tema"].notna() & (exploded["tema"] != "")]
    return (
        exploded.groupby("tema")
        .size()
        .rename_axis("tema")
        .reset_index(name="n")
        .sort_values("n", ascending=False)
        .reset_index(drop=True)
    )
