# =============================================================================
# pages/3_perfil_deputado.py
# Perfil individual de um deputado.
# Lê st.session_state["dep_id"] definido em 2_deputados.py.
# =============================================================================

import json
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import date

from utils.dados import (
    base_com_deputados,
    carregar_deputados,
    top_coautores,
    proposicoes_do_deputado,
    perfil_tematico,
    parse_list_safe,
)

st.markdown("""
<style>
    html, body, [class*="css"] { font-family: 'Inter, sans-serif; }
    .stApp { background-color: #F7F8FA; color: #1A2744; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E4E7EF; }
    section[data-testid="stSidebar"] * { color: #1A2744 !important; }
    .block-container { padding-top: 2rem; }

    .dep-header {
        display: flex; align-items: center; gap: 24px;
        background: #FFFFFF; border: 1px solid #E4E7EF;
        border-radius: 16px; padding: 24px 32px;
        margin-bottom: 24px;
    }
    .dep-foto-lg {
        width: 100px; height: 100px;
        border-radius: 50%; object-fit: cover;
        border: 3px solid #E4E7EF; flex-shrink: 0;
    }
    .dep-header-info { flex: 1; }
    .dep-nome-lg {
        font-family: 'Fraunces', serif;
        font-size: 28px; font-weight: 700; color: #1A2744;
        margin-bottom: 4px;
    }
    .dep-meta { font-size: 14px; color: #5C6B8A; margin-bottom: 8px; }
    .dep-tag {
        display: inline-block;
        background: #F0F2F7; border-radius: 6px;
        padding: 3px 10px; font-size: 12px; font-weight: 600;
        color: #1A2744; margin-right: 6px; margin-bottom: 4px;
    }
    .dep-rede {
        display: inline-block; font-size: 12px;
        color: #2563EB; margin-right: 12px;
        text-decoration: none;
    }

    .stat-card {
        background: #FFFFFF; border: 1px solid #E4E7EF;
        border-left: 3px solid #1A2744;
        border-radius: 12px; padding: 16px 20px;
    }
    .stat-label {
        font-size: 10px; font-weight: 600;
        letter-spacing: 0.1em; text-transform: uppercase;
        color: #5C6B8A; margin-bottom: 4px;
    }
    .stat-valor {
        font-family: 'Fraunces', serif;
        font-size: 28px; font-weight: 700; color: #1A2744;
    }
    .stat-sub { font-size: 12px; color: #5C6B8A; margin-top: 2px; }

    .secao {
        font-size: 11px; font-weight: 600;
        letter-spacing: 0.12em; text-transform: uppercase;
        color: #9BA8C0; margin: 32px 0 12px 0;
        padding-bottom: 8px; border-bottom: 1px solid #E4E7EF;
    }

    .coautor-card {
        background: #FFFFFF; border: 1px solid #E4E7EF;
        border-radius: 12px; padding: 12px 16px;
        display: flex; align-items: center; gap: 12px;
        margin-bottom: 8px;
    }
    .coautor-foto {
        width: 44px; height: 44px; border-radius: 50%;
        object-fit: cover; border: 2px solid #E4E7EF; flex-shrink: 0;
    }
    .coautor-nome { font-size: 13px; font-weight: 600; color: #1A2744; }
    .coautor-info { font-size: 12px; color: #5C6B8A; }
    .coautor-n {
        margin-left: auto; font-family: 'Fraunces', serif;
        font-size: 20px; font-weight: 700; color: #1A2744;
        text-align: right;
    }
    .coautor-n-label { font-size: 10px; color: #9BA8C0; }

    .prop-row {
        background: #FFFFFF; border: 1px solid #E4E7EF;
        border-radius: 8px; padding: 12px 16px; margin-bottom: 6px;
    }
    .prop-tipo {
        font-size: 11px; font-weight: 600; color: #5C6B8A;
        text-transform: uppercase; letter-spacing: 0.05em;
    }
    .prop-ementa { font-size: 13px; color: #1A2744; margin: 4px 0; }
    .prop-sit-norma  { color: #2D6A4F; font-size: 11px; font-weight: 600; }
    .prop-sit-tram   { color: #2563EB; font-size: 11px; font-weight: 600; }
    .prop-sit-arq    { color: #DC2626; font-size: 11px; font-weight: 600; }
            
    /* Barra superior do Streamlit */
    header[data-testid="stHeader"] {
        background-color: #F7F8FA !important;
    }

    /* Remove sombra/linha se houver */
    header[data-testid="stHeader"]::before {
        background: none !important;
    }
            
        /* Sidebar inteira */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E4E7EF !important;
    }

    /* Tudo dentro da sidebar */
    section[data-testid="stSidebar"] * {
        background-color: transparent !important;
        color: #1A2744 !important;
    }

    /* Inputs dentro da sidebar */
    section[data-testid="stSidebar"] [data-baseweb="select"],
    section[data-testid="stSidebar"] [data-baseweb="input"] {
        background-color: #FFFFFF !important;
    }

    /* Tags selecionadas (multiselect) */
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background-color: #F0F2F7 !important;
        color: #1A2744 !important;
    }
    /* Fundo global */
    html, body {
        background-color: #FFFFFF !important;
    }

    .stApp {
        background-color: #FFFFFF !important;
    }

    /* Container principal */
    .main, .block-container {
        background-color: #FFFFFF !important;
    }
    /* Container do select/multiselect */
section[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #FFFFFF !important;
    border: 1px solid #E4E7EF !important;
}

/* Área onde aparece o valor selecionado */
section[data-testid="stSidebar"] [data-baseweb="select"] div {
    background-color: #FFFFFF !important;
    color: #1A2744 !important;
}

/* Texto dentro do select */
section[data-testid="stSidebar"] [data-baseweb="select"] span {
    color: #1A2744 !important;
}

    /* Tags (itens selecionados no multiselect) */
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background-color: #F0F2F7 !important;
        color: #1A2744 !important;
        border: 1px solid #E4E7EF !important;
    }

    /* Botão de remover (x) dentro da tag */
    section[data-testid="stSidebar"] [data-baseweb="tag"] svg {
        fill: #1A2744 !important;
    }

    /* Dropdown aberto (lista de opções) */
    [data-baseweb="menu"] {
        background-color: #FFFFFF !important;
    }

    [data-baseweb="menu"] li {
        background-color: #FFFFFF !important;
        color: #1A2744 !important;
    }

    /* Hover nas opções */
    [data-baseweb="menu"] li:hover {
        background-color: #F0F2F7 !important;
    }
</style>
""", unsafe_allow_html=True)

PLOT_LAYOUT = dict(
    paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
    font=dict(family="Inter, sans-serif", color="#1A2744", size=12),
    margin=dict(l=0, r=0, t=32, b=0),
)

# -----------------------------------------------------------------------------
# LÊ DEP_ID — session_state (navegação normal) ou query_params (link direto)
# URL compartilhável: /Perfil?dep_id=204478
# -----------------------------------------------------------------------------
dep_id_str = (
    st.session_state.get("dep_id")
    or st.query_params.get("dep_id")
)
if not dep_id_str:
    st.warning("Nenhum deputado selecionado. Volte para a página de Deputados.")
    if st.button("← Ir para Deputados"):
        st.switch_page("pages/2_deputados.py")
    st.stop()

try:
    dep_id = int(dep_id_str)
    # sincroniza query_params com o ID atual para URL compartilhável
    st.query_params["dep_id"] = str(dep_id)
except ValueError:
    st.error("ID de deputado inválido.")
    st.stop()

# -----------------------------------------------------------------------------
# SIDEBAR
# -----------------------------------------------------------------------------
with st.sidebar:
    img_path = Path(__file__).parent.parent / "congresso.png"
    if img_path.exists():
        st.image(str(img_path), use_container_width=True)
    st.markdown("### Câmara Data")
    st.caption("Produção legislativa desde 2019")
    st.markdown("---")
    if st.button("← Voltar para Deputados"):
        st.switch_page("pages/2_deputados.py")

# -----------------------------------------------------------------------------
# CARREGAMENTO
# -----------------------------------------------------------------------------
with st.spinner("Carregando perfil..."):
    deps     = carregar_deputados()
    df_base  = base_com_deputados()

dep_row = deps[deps["id"] == dep_id]
if dep_row.empty:
    st.error(f"Deputado {dep_id} não encontrado.")
    st.stop()

dep = dep_row.iloc[0]

# proposições como autor principal
props_principal = proposicoes_do_deputado(df_base, dep_id, modo="principal")
# proposições como coautor (não principal)
props_coautor   = proposicoes_do_deputado(df_base, dep_id, modo="coautor")
# todas
props_todas     = proposicoes_do_deputado(df_base, dep_id, modo="ambos")

# -----------------------------------------------------------------------------
# HEADER DO DEPUTADO
# -----------------------------------------------------------------------------
foto_url = dep.get("urlFoto") or "https://www.camara.leg.br/internet/deputado/bandSem_foto.jpg"
nome     = dep.get("nome") or "—"
partido  = dep.get("siglaPartido") or "—"
uf       = dep.get("siglaUf") or "—"
leg      = int(dep.get("idLegislatura") or 0)
escol    = dep.get("escolaridade") or "—"
sexo     = "Masculino" if dep.get("sexo") == "M" else "Feminino" if dep.get("sexo") == "F" else "—"
fed      = dep.get("federacao") or partido
website  = dep.get("urlWebsite")

# idade
dn = dep.get("dataNascimento")
if pd.notna(dn) and dn is not None:
    try:
        dn_date = pd.Timestamp(dn).date()
        hoje    = date.today()
        idade   = hoje.year - dn_date.year - ((hoje.month, hoje.day) < (dn_date.month, dn_date.day))
        idade_str = f"{idade} anos"
        nasc_str  = pd.Timestamp(dn).strftime("%d/%m/%Y")
    except Exception:
        idade_str = "—"
        nasc_str  = "—"
else:
    idade_str = "—"
    nasc_str  = "—"

# redes sociais
redes_raw = dep.get("redeSocial") or "[]"
try:
    redes = json.loads(redes_raw) if isinstance(redes_raw, str) else redes_raw
except Exception:
    redes = []

def rede_label(url: str) -> str:
    url_l = url.lower()
    if "instagram" in url_l: return "Instagram"
    if "twitter"   in url_l or "x.com" in url_l: return "Twitter/X"
    if "facebook"  in url_l: return "Facebook"
    if "youtube"   in url_l: return "YouTube"
    if "tiktok"    in url_l: return "TikTok"
    return "Site"

redes_html = "".join([
    f'<a class="dep-rede" href="{r}" target="_blank">{rede_label(r)}</a>'
    for r in redes
])
if website:
    redes_html += f'<a class="dep-rede" href="{website}" target="_blank">Website</a>'

st.markdown(f"""
<div class="dep-header">
    <img class="dep-foto-lg" src="{foto_url}">
    <div class="dep-header-info">
        <div class="dep-nome-lg">{nome}</div>
        <div class="dep-meta">{partido} · {uf} · Legislatura {leg}</div>
        <div style="margin-bottom:8px">
            <span class="dep-tag">{fed}</span>
            <span class="dep-tag">{escol}</span>
            <span class="dep-tag">{idade_str}</span>
            <span class="dep-tag">{sexo}</span>
        </div>
        <div>{redes_html}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# CARDS DE STATS
# -----------------------------------------------------------------------------
n_principal = len(props_principal)
n_coautor   = len(props_coautor)
n_total     = n_principal + n_coautor

n_normas  = (props_principal["situacao_simplificada"] == "Transformada em norma jurídica").sum()
n_tram    = (props_principal["situacao_simplificada"] == "Em tramitação").sum()
n_arq     = (props_principal["situacao_simplificada"] == "Arquivada").sum()
taxa      = n_normas / (n_normas + n_arq) * 100 if (n_normas + n_arq) > 0 else 0

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Autor principal</div>
        <div class="stat-valor">{n_principal}</div>
        <div class="stat-sub">proposições apresentadas</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Coautorias</div>
        <div class="stat-valor">{n_coautor}</div>
        <div class="stat-sub">proposições coassinadas</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Viraram norma</div>
        <div class="stat-valor">{n_normas}</div>
        <div class="stat-sub">como autor principal</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Taxa de efetividade</div>
        <div class="stat-valor">{taxa:.1f}%</div>
        <div class="stat-sub">das finalizadas viraram norma</div>
    </div>""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# PERFIL TEMÁTICO + TOP COAUTORES
# -----------------------------------------------------------------------------
st.markdown("")
col_pizza, col_coautores = st.columns([1, 1])

with col_pizza:
    st.markdown('<div class="secao">Perfil temático</div>', unsafe_allow_html=True)

    temas_df = perfil_tematico(props_todas)

    if not temas_df.empty:
        # agrupa temas menores em "Outros" para não poluir a pizza
        TOP_TEMAS = 8
        if len(temas_df) > TOP_TEMAS:
            top    = temas_df.head(TOP_TEMAS)
            outros = pd.DataFrame([{"tema": "Outros", "n": temas_df.iloc[TOP_TEMAS:]["n"].sum()}])
            temas_df = pd.concat([top, outros], ignore_index=True)

        fig_pizza = go.Figure(go.Pie(
            labels=temas_df["tema"],
            values=temas_df["n"],
            hole=0.45,
            textinfo="percent",
            hovertemplate="<b>%{label}</b><br>%{value} proposições (%{percent})<extra></extra>",
            marker=dict(
                colors=[
                    "#1A2744", "#2563EB", "#2D6A4F", "#F4B942",
                    "#DC2626", "#7C3AED", "#0891B2", "#D97706", "#9CA3AF"
                ]
            ),
        ))
        fig_pizza.update_layout(
            **PLOT_LAYOUT,
            height=340,
            showlegend=True,
            legend=dict(
                orientation="v", x=1.02, y=0.5,
                font=dict(size=11, color="#1A2744"),
            ),
            annotations=[dict(
                text=f"<b>{n_total}</b><br>props",
                x=0.5, y=0.5, font_size=14,
                font_color="#1A2744", showarrow=False
            )]
        )
        st.plotly_chart(fig_pizza, use_container_width=True)
    else:
        st.info("Nenhuma proposição encontrada para este deputado.")

with col_coautores:
    st.markdown('<div class="secao">Top 5 coautores</div>', unsafe_allow_html=True)

    coautores = top_coautores(dep_id, n=5)

    if coautores:
        for c in coautores:
            foto = c.get("urlFoto") or "https://www.camara.leg.br/internet/deputado/bandSem_foto.jpg"
            st.markdown(f"""
            <div class="coautor-card">
                <img class="coautor-foto" src="{foto}">
                <div>
                    <div class="coautor-nome">{c['nome']}</div>
                    <div class="coautor-info">{c['siglaPartido']} · {c['siglaUf']}</div>
                </div>
                <div class="coautor-n">
                    {c['n_proposicoes']}
                    <div class="coautor-n-label">proposições juntos</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Nenhuma coautoria registrada.")

# -----------------------------------------------------------------------------
# PROPOSIÇÕES DO DEPUTADO
# -----------------------------------------------------------------------------
st.markdown('<div class="secao">Proposições como autor principal</div>', unsafe_allow_html=True)

if props_principal.empty:
    st.info("Nenhuma proposição como autor principal.")
else:
    # filtro rápido de situação
    sit_filtro = st.multiselect(
        "Filtrar por situação",
        options=["Em tramitação", "Arquivada", "Transformada em norma jurídica"],
        default=["Em tramitação", "Arquivada", "Transformada em norma jurídica"],
        key="sit_props",
    )

    props_show = props_principal[
        props_principal["situacao_simplificada"].isin(sit_filtro)
    ].sort_values("dataApresentacao", ascending=False).head(50)

    def cor_sit(sit):
        if sit == "Transformada em norma jurídica": return "prop-sit-norma"
        if sit == "Em tramitação":                  return "prop-sit-tram"
        return "prop-sit-arq"

    for _, row in props_show.iterrows():
        sit  = row.get("situacao_simplificada") or "—"
        tipo = f"{row.get('siglaTipo','—')} {row.get('numero','')}/{row.get('ano','')}"
        data = pd.Timestamp(row["dataApresentacao"]).strftime("%d/%m/%Y") if pd.notna(row.get("dataApresentacao")) else "—"
        ementa = (row.get("ementa") or "—")[:200]
        css_sit = cor_sit(sit)

        st.markdown(f"""
        <div class="prop-row">
            <div class="prop-tipo">{tipo} · {data}</div>
            <div class="prop-ementa">{ementa}</div>
            <div class="{css_sit}">{sit}</div>
        </div>
        """, unsafe_allow_html=True)

    if len(props_principal) > 50:
        st.caption(f"Mostrando 50 de {len(props_principal)} proposições.")