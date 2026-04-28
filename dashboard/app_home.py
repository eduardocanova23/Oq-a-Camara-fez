# =============================================================================
# app_home.py
# Página inicial do dashboard Câmara Data.
# =============================================================================

from pathlib import Path
import streamlit as st

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,400;0,700;1,400&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .stApp { background-color: #F7F8FA; color: #1A2744; }
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E4E7EF;
    }
    section[data-testid="stSidebar"] * { color: #1A2744 !important; }
    .block-container { padding-top: 2rem; }

    .hero-title {
        font-family: 'Fraunces', serif;
        font-size: 42px; font-weight: 700;
        color: #1A2744; margin: 24px 0 8px 0; line-height: 1.2;
    }
    .hero-sub {
        font-size: 18px; color: #5C6B8A;
        font-weight: 300; margin-bottom: 32px;
    }
    .nav-card {
        background: #FFFFFF; border: 1px solid #E4E7EF;
        border-radius: 12px; padding: 20px 24px; margin-bottom: 8px;
    }
    .nav-card-title {
        font-family: 'Fraunces', serif;
        font-size: 18px; font-weight: 700;
        color: #1A2744; margin-bottom: 6px;
    }
    .nav-card-desc { font-size: 14px; color: #5C6B8A; line-height: 1.5; }
    .footer-line {
        font-size: 12px; color: #9BA8C0;
        margin-top: 40px; padding-top: 16px;
        border-top: 1px solid #E4E7EF;
    }
    /* Barra superior do Streamlit */
    header[data-testid="stHeader"] {
        background-color: #F7F8FA !important;
    }

    /* Remove sombra/linha se houver */
    header[data-testid="stHeader"]::before {
        background: none !important;
    }
</style>
""", unsafe_allow_html=True)

# sidebar
with st.sidebar:
    img_path = Path(__file__).parent / "congresso.png"
    if img_path.exists():
        st.image(str(img_path), use_container_width=True)
    st.caption("Produção legislativa desde 2019")

# hero image
img_path = Path(__file__).parent / "congresso.png"
if img_path.exists():
    st.image(str(img_path), use_container_width=True)

st.markdown("""
<div class="hero-title">O que a Câmara<br>efetivamente fez?</div>
<div class="hero-sub">Explore a produção legislativa dos deputados federais desde 2019.</div>
""", unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-card-title">📊 Painel Geral</div>
        <div class="nav-card-desc">Visão macro do período: o que foi apresentado, aprovado e arquivado por tema.</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-card-title">⏱️ Tramitação</div>
        <div class="nav-card-desc">Quais temas demoram mais. O que fica parado. O que tem prioridade na pauta.</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-card-title">👤 Deputados</div>
        <div class="nav-card-desc">Perfil temático, taxa de aprovação e todas as proposições de cada deputado.</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div class="nav-card">
        <div class="nav-card-title">🔍 Explorador</div>
        <div class="nav-card-desc">Tabela completa com filtros avançados para mergulhar nos dados.</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("""
<div class="footer-line">
    Dados: API aberta da Câmara dos Deputados &nbsp;·&nbsp;
    Atualização semanal &nbsp;·&nbsp;
    Proposições desde jan/2019
</div>
""", unsafe_allow_html=True)