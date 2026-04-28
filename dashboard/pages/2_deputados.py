# =============================================================================
# pages/2_deputados.py
# Grade de cards de deputados com busca e filtros.
# Ao clicar num deputado, redireciona para 3_perfil_deputado.py via session_state.
# =============================================================================

import streamlit as st
import pandas as pd
from pathlib import Path

from utils.dados import carregar_deputados

st.set_page_config(
    page_title="Deputados — Câmara Data",
    page_icon="🇧🇷",
    layout="wide",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .stApp { background-color: #F7F8FA; color: #1A2744; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E4E7EF; }
    section[data-testid="stSidebar"] * { color: #1A2744 !important; }
    section[data-testid="stSidebar"] [data-baseweb="select"] div,
    section[data-testid="stSidebar"] [data-baseweb="input"] {
        background-color: #FFFFFF !important; color: #1A2744 !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background-color: #E4E7EF !important; color: #1A2744 !important;
    }
    [data-baseweb="popover"] { background-color: #FFFFFF !important; color: #1A2744 !important; }
    [data-baseweb="menu"] li { background-color: #FFFFFF !important; color: #1A2744 !important; }
    .block-container { padding-top: 2rem; }

    .page-title {
        font-family: 'Fraunces', serif;
        font-size: 32px; font-weight: 700;
        color: #1A2744; margin-bottom: 2px;
    }
    .page-subtitle { font-size: 14px; color: #5C6B8A; margin-bottom: 24px; }

    /* card de deputado */
    .dep-card {
        background: #FFFFFF;
        border: 1px solid #E4E7EF;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        cursor: pointer;
        transition: border-color 0.15s, box-shadow 0.15s;
        height: 200px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 8px;
    }
    .dep-card:hover {
        border-color: #1A2744;
        box-shadow: 0 4px 12px rgba(26,39,68,0.08);
    }
    .dep-foto {
        width: 64px; height: 64px;
        border-radius: 50%;
        object-fit: cover;
        border: 2px solid #E4E7EF;
    }
    .dep-nome {
        font-family: 'Fraunces', serif;
        font-size: 13px; font-weight: 700;
        color: #1A2744; line-height: 1.3;
        max-width: 140px;
    }
    .dep-info {
        font-size: 11px; color: #5C6B8A;
    }
    .dep-partido {
        font-size: 11px; font-weight: 600;
        color: #1A2744;
        background: #F0F2F7;
        border-radius: 4px;
        padding: 2px 6px;
    }
    .secao {
        font-size: 11px; font-weight: 600;
        letter-spacing: 0.12em; text-transform: uppercase;
        color: #9BA8C0; margin: 24px 0 12px 0;
        padding-bottom: 8px; border-bottom: 1px solid #E4E7EF;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# CARREGAMENTO
# -----------------------------------------------------------------------------
with st.spinner("Carregando deputados..."):
    deps = carregar_deputados()

# -----------------------------------------------------------------------------
# SIDEBAR — FILTROS
# -----------------------------------------------------------------------------
with st.sidebar:
    img_path = Path(__file__).parent.parent / "congresso.png"
    if img_path.exists():
        st.image(str(img_path), use_container_width=True)
    st.markdown("### Câmara Data")
    st.caption("Produção legislativa desde 2019")
    st.markdown("---")
    st.markdown("**Filtros — Deputados**")

    leg_opts = {"Legislatura 57 (atual)": 57, "Legislatura 56": 56, "Todas": 0}
    leg_label = st.selectbox("Legislatura", options=list(leg_opts.keys()), index=0)
    leg_sel = leg_opts[leg_label]

    partidos_disp = sorted(deps["siglaPartido"].dropna().unique().tolist())
    partidos_sel = st.multiselect("Partido", options=partidos_disp, default=[], placeholder="Todos")

    ufs_disp = sorted(deps["siglaUf"].dropna().unique().tolist())
    ufs_sel = st.multiselect("UF", options=ufs_disp, default=[], placeholder="Todos os estados")

    sexo_opts = {"Todos": None, "Masculino": "M", "Feminino": "F"}
    sexo_label = st.selectbox("Sexo", options=list(sexo_opts.keys()))
    sexo_sel = sexo_opts[sexo_label]

    st.markdown("---")
    st.caption(f"{len(deps)} deputados cadastrados")

# -----------------------------------------------------------------------------
# FILTRAGEM
# -----------------------------------------------------------------------------
df = deps.copy()

if leg_sel != 0:
    df = df[df["idLegislatura"] == leg_sel]
if partidos_sel:
    df = df[df["siglaPartido"].isin(partidos_sel)]
if ufs_sel:
    df = df[df["siglaUf"].isin(ufs_sel)]
if sexo_sel:
    df = df[df["sexo"] == sexo_sel]

# -----------------------------------------------------------------------------
# CABEÇALHO + BUSCA
# -----------------------------------------------------------------------------
st.markdown('<div class="page-title">Deputados</div>', unsafe_allow_html=True)
st.markdown(f'<div class="page-subtitle">{len(df):,} deputados encontrados</div>', unsafe_allow_html=True)

busca = st.text_input("🔍 Buscar por nome", placeholder="Ex: Tabata Amaral, Nikolas...")

if busca:
    df = df[df["nome"].str.contains(busca, case=False, na=False)]

if df.empty:
    st.info("Nenhum deputado encontrado com os filtros selecionados.")
    st.stop()

# -----------------------------------------------------------------------------
# GRADE DE CARDS
# -----------------------------------------------------------------------------
st.markdown(f'<div class="secao">{len(df):,} deputados</div>', unsafe_allow_html=True)

COLS = 6
df_sorted = df.sort_values("nome").reset_index(drop=True)

for row_start in range(0, len(df_sorted), COLS):
    cols = st.columns(COLS)
    for col_idx, col in enumerate(cols):
        dep_idx = row_start + col_idx
        if dep_idx >= len(df_sorted):
            break
        dep = df_sorted.iloc[dep_idx]

        with col:
            foto_url = dep.get("urlFoto") or "https://www.camara.leg.br/internet/deputado/bandSem_foto.jpg"
            partido  = dep.get("siglaPartido") or "—"
            uf       = dep.get("siglaUf") or "—"
            leg      = int(dep.get("idLegislatura") or 0)

            st.markdown(f"""
            <div class="dep-card">
                <img class="dep-foto" src="{foto_url}" onerror="this.src='https://www.camara.leg.br/internet/deputado/bandSem_foto.jpg'">
                <div class="dep-nome">{dep['nome']}</div>
                <div class="dep-partido">{partido} · {uf}</div>
                <div class="dep-info">Leg. {leg}</div>
            </div>
            """, unsafe_allow_html=True)

            if st.button("Ver perfil", key=f"dep_{dep['id']}", use_container_width=True):
                st.session_state["dep_id"]   = int(dep["id"])
                st.session_state["dep_nome"] = dep["nome"]
                st.switch_page("pages/_perfil_deputado.py")