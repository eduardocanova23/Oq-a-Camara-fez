# =============================================================================
# utils/dados.py
# Carrega e filtra os dados do pipeline para o dashboard.
#
# Todas as páginas do dashboard importam daqui — nunca leem parquets diretamente.
# Isso centraliza a lógica de dados e facilita a migração futura para Supabase.
# =============================================================================

import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------------
# CAMINHOS
# -----------------------------------------------------------------------------
# Caminho relativo: dashboard/ está um nível abaixo de oq-a-camara-fez/
# pipeline/ está no mesmo nível que dashboard/
_ROOT = Path(__file__).parent.parent.parent  # sobe de utils/ → dashboard/ → oq-a-camara-fez/
_PIPELINE_FINAL = _ROOT / "pipeline" / "data" / "final"

PATH_BASE        = _PIPELINE_FINAL / "base_legislativa.parquet"
PATH_DEPUTADOS   = _PIPELINE_FINAL / "deputados.parquet"

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

# -----------------------------------------------------------------------------
# CARREGAMENTO (com cache do Streamlit)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def carregar_base() -> pd.DataFrame:
    """
    Carrega a base legislativa e faz o pré-processamento necessário.
    Cache de 1h — atualiza automaticamente depois desse período.
    """
    df = pd.read_parquet(PATH_BASE)

    # datas
    df["dataApresentacao"]      = pd.to_datetime(df["dataApresentacao"],      errors="coerce")
    df["ultimoStatus_dataHora"] = pd.to_datetime(df["ultimoStatus_dataHora"], errors="coerce")

    # ano e mês de apresentação (úteis para agrupamentos)
    df["ano_apresentacao"] = df["dataApresentacao"].dt.year
    df["mes_apresentacao"] = df["dataApresentacao"].dt.to_period("M").dt.to_timestamp()

    # temas como lista Python
    df["_temas_lista"] = df["temas_tuplas_json"].apply(_parse_temas)

    # autor principal (primeiro deputado por ordemAssinatura)
    df["_autor_principal"] = df["autores_deputados"].apply(_extrair_autor_principal)

    # tempo de tramitação em dias
    df["_dias_tramitacao"] = _calcular_dias_tramitacao(df)

    return df


@st.cache_data(ttl=3600)
def carregar_deputados() -> pd.DataFrame:
    """Carrega o DataFrame de deputados."""
    df = pd.read_parquet(PATH_DEPUTADOS)
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    return df


# -----------------------------------------------------------------------------
# HELPERS DE PARSING
# -----------------------------------------------------------------------------

def _parse_temas(x) -> list[str]:
    """Extrai lista de nomes de temas de temas_tuplas_json."""
    if pd.isna(x) or x is None:
        return []
    try:
        dados = json.loads(x) if isinstance(x, str) else x
        return [str(t[0]).strip() for t in dados if isinstance(t, list) and len(t) >= 1 and t[0]]
    except Exception:
        return []


def _extrair_autor_principal(x) -> dict | None:
    """Extrai o autor com menor ordemAssinatura de autores_deputados."""
    if pd.isna(x) or x is None:
        return None
    try:
        autores = json.loads(x) if isinstance(x, str) else x
        if not autores:
            return None
        # ordena por ordemAssinatura, pega o primeiro
        autores_validos = [a for a in autores if isinstance(a, dict)]
        if not autores_validos:
            return None
        return min(autores_validos, key=lambda a: a.get("ordemAssinatura") or 999)
    except Exception:
        return None


def _calcular_dias_tramitacao(df: pd.DataFrame) -> pd.Series:
    """
    Calcula tempo de tramitação em dias:
    - Para normas e arquivadas: dataApresentacao → ultimoStatus_dataHora
    - Para em tramitação: dataApresentacao → hoje
    """
    hoje = pd.Timestamp.today().normalize()

    data_fim = df["ultimoStatus_dataHora"].copy()

    # proposições em tramitação: data fim = hoje
    mask_tram = df["situacao_simplificada"] == "Em tramitação"
    data_fim[mask_tram] = hoje

    dias = (data_fim - df["dataApresentacao"]).dt.days
    return dias.clip(lower=0)   # evita negativos por inconsistência de datas


# -----------------------------------------------------------------------------
# ENRIQUECIMENTO COM DEPUTADOS
# -----------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def base_com_deputados() -> pd.DataFrame:
    """
    Junta a base legislativa com os dados de deputados pelo autor principal.
    Adiciona colunas: partido_autor, uf_autor, foto_autor.
    """
    df   = carregar_base().copy()
    deps = carregar_deputados()[["id", "siglaPartido", "siglaUf", "urlFoto", "federacao"]].copy()
    deps.columns = ["id_dep", "partido_autor", "uf_autor", "foto_autor", "federacao_autor"]

    # extrai id do autor principal
    df["_id_autor"] = df["_autor_principal"].apply(
        lambda a: a.get("idDeputado") if isinstance(a, dict) else None
    )
    df["_id_autor"] = pd.to_numeric(df["_id_autor"], errors="coerce").astype("Int64")

    df = df.merge(
        deps.rename(columns={"id_dep": "_id_autor"}),
        on="_id_autor",
        how="left",
    )

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
) -> pd.DataFrame:
    """
    Aplica filtros ao DataFrame. Todos os parâmetros são opcionais.
    Retorna o DataFrame filtrado.
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
        # proposição entra se tem pelo menos um dos temas selecionados
        mask &= df["_temas_lista"].apply(lambda lst: bool(set(lst) & set(temas)))

    if partidos and "partido_autor" in df.columns:
        mask &= df["partido_autor"].isin(partidos)

    if ufs and "uf_autor" in df.columns:
        mask &= df["uf_autor"].isin(ufs)

    return df[mask].copy()


# -----------------------------------------------------------------------------
# AGREGAÇÕES PRONTAS (usadas por múltiplas páginas)
# -----------------------------------------------------------------------------

def contagem_por_situacao(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna contagem e percentual por situação."""
    cont = (
        df["situacao_simplificada"]
        .value_counts()
        .rename_axis("situacao")
        .reset_index(name="n")
    )
    cont["pct"] = (cont["n"] / cont["n"].sum() * 100).round(1)
    return cont


def contagem_por_tema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna contagem de proposições por tema (explode a lista de temas).
    Cada proposição pode contar para múltiplos temas.
    """
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
    cont = cont.sort_values("total", ascending=False).reset_index(drop=True)
    return cont


def serie_temporal(df: pd.DataFrame, freq: str = "M") -> pd.DataFrame:
    """
    Retorna contagem de proposições apresentadas por período.
    freq: 'M' (mensal) ou 'Y' (anual)
    """
    col = "mes_apresentacao" if freq == "M" else "ano_apresentacao"
    if freq == "Y":
        df = df.copy()
        df["_periodo"] = df["dataApresentacao"].dt.year
    else:
        df = df.copy()
        df["_periodo"] = df["mes_apresentacao"]

    return (
        df.groupby("_periodo")
        .size()
        .rename_axis("periodo")
        .reset_index(name="n")
        .sort_values("periodo")
    )


def tempo_tramitacao_por_tema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna tempo médio de tramitação (em dias) por tema.
    Usa apenas proposições com estado final (norma ou arquivada).
    """
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
        .query("n >= 10")   # só temas com pelo menos 10 proposições finalizadas
        .sort_values("mediana_dias", ascending=False)
        .reset_index(drop=True)
    )


def lista_temas_disponiveis(df: pd.DataFrame) -> list[str]:
    """Retorna lista ordenada de todos os temas presentes no DataFrame."""
    todos = set()
    for lst in df["_temas_lista"]:
        todos.update(lst)
    return sorted(todos)


def lista_partidos_disponiveis(df: pd.DataFrame) -> list[str]:
    """Retorna lista ordenada de partidos presentes no DataFrame."""
    if "partido_autor" not in df.columns:
        return []
    return sorted(df["partido_autor"].dropna().unique().tolist())