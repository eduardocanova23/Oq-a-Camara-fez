# =============================================================================
# pages/1_painel_geral.py
# Visão macro da produção legislativa da Câmara dos Deputados.
# Responde: "O que a Câmara efetivamente fez nesse período?"
# =============================================================================

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.dados import (
    base_com_deputados,
    contagem_por_tema,
    contar_deputados_envolvidos,
    filtrar,
    lista_partidos_disponiveis,
    tempo_tramitacao_por_tema,
    COR_SITUACAO,
    GRUPOS_AUTOR,
    MODOS_PARTIDO,
    SITUACOES,
    TIPOS,
)

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Painel Geral — Câmara Data",
    page_icon="🇧🇷",
    layout="wide",
)

from utils.style import apply_global_style
apply_global_style()

# -----------------------------------------------------------------------------
# ESTILO — claro, editorial
# -----------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,400;0,700;1,400&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    .stApp {
        background-color: #F7F8FA;
        color: #1A2744;
    }
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E4E7EF;
    }
    section[data-testid="stSidebar"] * {
        color: #1A2744 !important;
    }
    
    /* fundo do multiselect e date */
    section[data-testid="stSidebar"] [data-baseweb="select"] div,
    section[data-testid="stSidebar"] [data-baseweb="input"] {
        background-color: #FFFFFF !important;
        color: #1A2744 !important;
    }

    /* tags selecionadas no multiselect */
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background-color: #E4E7EF !important;
        color: #1A2744 !important;
    }

    section[data-testid="stSidebar"] [data-baseweb="select"] {
        overflow: visible !important;
    }

    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        margin-top: 4px !important;
        margin-bottom: 4px !important;
    }
            
    section[data-testid="stSidebar"] [data-baseweb="tag"]:first-child {
        padding-left: 4px !important;
        margin-left: 16px !important;
    }
            
    /* dropdown das opções */
    [data-baseweb="popover"] {
        background-color: #FFFFFF !important;
        color: #1A2744 !important;
    }

    [data-baseweb="menu"] li {
        background-color: #FFFFFF !important;
        color: #1A2744 !important;
    }
            
    .block-container {
        padding-top: 2rem;
    }

    /* cards de métrica */
    div[data-testid="metric-container"] {
        background: #FFFFFF;
        border: 1px solid #E4E7EF;
        border-radius: 12px;
        padding: 16px 20px;
    }
    div[data-testid="metric-container"] label {
        color: #1A2744 !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #1A2744 !important;
        font-family: 'Fraunces', serif !important;
        font-size: 28px !important;
        font-weight: 700 !important;
    }
    div[data-testid="metric-container"] * {
        color: #1A2744 !important;
    }
            
    [class*="st-emotion-cache"] [data-testid="stMetricValue"],
    [class*="st-emotion-cache"] [data-testid="stMetricLabel"] {
        color: #1A2744 !important;
    }

    /* destaque narrativo */
    .destaque-box {
        background: #FFFFFF;
        border: 1px solid #E4E7EF;
        border-left: 3px solid #1A2744;
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }
    .destaque-label {
        color: #1A2744;
        font-size: 10px;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .destaque-valor {
        color: #1A2744;
        font-family: 'Fraunces', serif;
        font-size: 22px;
        font-weight: 700;
    }
    .destaque-sub {
        color: #1A2744;
        font-size: 12px;
        margin-top: 2px;
    }

    /* título da página */
    .page-title {
        font-family: 'Fraunces', serif;
        font-size: 32px;
        font-weight: 700;
        color: #1A2744;
        margin-bottom: 2px;
    }
    .page-subtitle {
        font-size: 14px;
        color: #1A2744;
        margin-bottom: 24px;
    }

    /* divisor de seção */
    .secao {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #9BA8C0;
        margin: 32px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #E4E7EF;
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

# layout de gráficos — tema claro consistente
PLOT_LAYOUT = dict(
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FFFFFF",
    font=dict(family="DM Sans, sans-serif", color="#1A2744", size=12),
    margin=dict(l=0, r=0, t=32, b=0),
)

COR_SITUACAO_CLARO = {
    "Em tramitação":                  "#2563EB",
    "Arquivada":                      "#ff4d4d",
    "Transformada em norma jurídica": "#2D6A4F",
}

# -----------------------------------------------------------------------------
# CARREGAMENTO
# -----------------------------------------------------------------------------
with st.spinner("Carregando dados..."):
    df_full = base_com_deputados()

# -----------------------------------------------------------------------------
# SIDEBAR — FILTROS
# -----------------------------------------------------------------------------
with st.sidebar:
    from pathlib import Path
    img_path = Path(__file__).parent.parent / "congresso.png"
    if img_path.exists():
        st.image(str(img_path), use_container_width=True)

    st.markdown("### Câmara Data")
    st.caption("Produção legislativa desde 2019")
    st.markdown("---")
    st.markdown("**Filtros — Painel Geral**")

    leg_atual_inicio = pd.Timestamp("2023-02-01")
    leg_atual_fim    = pd.Timestamp.today()

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        data_inicio = st.date_input(
            "De",
            value=leg_atual_inicio.date(),
            min_value=pd.Timestamp("2019-01-01").date(),
            max_value=pd.Timestamp.today().date(),
        )
    with col_d2:
        data_fim = st.date_input(
            "Até",
            value=leg_atual_fim.date(),
            min_value=pd.Timestamp("2019-01-01").date(),
            max_value=pd.Timestamp.today().date(),
        )

    st.markdown("---")

    tipos_sel = st.multiselect(
        "Tipo de proposição",
        options=TIPOS,
        default=TIPOS,
    )

    situacoes_sel = st.multiselect(
        "Situação",
        options=SITUACOES,
        default=SITUACOES,
    )

    grupos_autor_sel = st.multiselect(
        "Tipo de autor",
        options=list(GRUPOS_AUTOR.keys()),
        default=list(GRUPOS_AUTOR.keys()),
    )

    # modo de contagem e filtro de partido só aparecem quando Deputado(a) está selecionado
    partidos_sel = []
    modo_partido = "principal"
    if not grupos_autor_sel or "Deputado(a)" in grupos_autor_sel:
        modo_partido_label = st.radio(
            "Autoria",
            options=list(MODOS_PARTIDO.keys()),
            index=0,
            help="'Só o autor principal' usa apenas quem assinou primeiro. 'Todos que tiveram coautoria' inclui qualquer coautor deputado.",
        )
        modo_partido = MODOS_PARTIDO[modo_partido_label]

        partidos_disponiveis = lista_partidos_disponiveis(df_full)
        partidos_sel = st.multiselect(
            "Partido do autor",
            options=partidos_disponiveis,
            default=[],
            placeholder="Todos os partidos",
        )

    st.markdown("---")
    ultima = df_full["ultimoStatus_dataHora"].max()
    if pd.notna(ultima):
        st.caption(f"Base atualizada em {ultima.strftime('%d/%m/%Y')}")

# -----------------------------------------------------------------------------
# FILTRAGEM
# -----------------------------------------------------------------------------
df = filtrar(
    df_full,
    data_inicio=pd.Timestamp(data_inicio),
    data_fim=pd.Timestamp(data_fim),
    situacoes=situacoes_sel if situacoes_sel else None,
    tipos=tipos_sel if tipos_sel else None,
    partidos=partidos_sel if partidos_sel else None,
    modo_partido=modo_partido,
    grupos_autor=grupos_autor_sel if grupos_autor_sel else None,
)

# -----------------------------------------------------------------------------
# CABEÇALHO
# -----------------------------------------------------------------------------
st.markdown('<div class="page-title">Painel Geral</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="page-subtitle">'
    f'{data_inicio.strftime("%d/%m/%Y")} → {data_fim.strftime("%d/%m/%Y")}'
    f' &nbsp;·&nbsp; {len(df):,} proposições'
    f'</div>',
    unsafe_allow_html=True
)

if df.empty:
    st.warning("Nenhuma proposição encontrada com os filtros selecionados.")
    st.stop()

# -----------------------------------------------------------------------------
# CARDS DE MÉTRICAS
# -----------------------------------------------------------------------------
n_total      = len(df)
n_normas     = (df["situacao_simplificada"] == "Transformada em norma jurídica").sum()
n_arquivadas = (df["situacao_simplificada"] == "Arquivada").sum()
n_tramitando = (df["situacao_simplificada"] == "Em tramitação").sum()

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Total de proposições</div>
        <div class="destaque-valor">{n_total:,}</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Viraram norma</div>
        <div class="destaque-valor">{n_normas:,}</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Arquivadas</div>
        <div class="destaque-valor">{n_arquivadas:,}</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Em tramitação</div>
        <div class="destaque-valor">{n_tramitando:,}</div>
    </div>
    """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# LINHA 2 — CARDS DE ANÁLISE
# -----------------------------------------------------------------------------
st.markdown("")

df_finais   = df[df["situacao_simplificada"].isin(["Transformada em norma jurídica", "Arquivada"])]
tempo_medio = df_finais["_dias_tramitacao"].median() if not df_finais.empty else 0
tempo_anos  = tempo_medio / 365

temas_cont  = contagem_por_tema(df)
tema_top    = temas_cont.iloc[0]["tema"] if not temas_cont.empty else "—"
tema_top_n  = int(temas_cont.iloc[0]["total"]) if not temas_cont.empty else 0
taxa_efet   = n_normas / (n_normas + n_arquivadas) * 100 if (n_normas + n_arquivadas) > 0 else 0

ids_deps = contar_deputados_envolvidos(
    df,
    partidos=partidos_sel or None,
    modo_partido=modo_partido,
)

d1, d2, d3, d4 = st.columns(4)

with d1:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Tempo mediano de tramitação</div>
        <div class="destaque-valor">{tempo_medio:.0f} dias</div>
        <div class="destaque-sub">≈ {tempo_anos:.1f} anos · proposições com desfecho final</div>
    </div>
    """, unsafe_allow_html=True)

with d2:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Tema mais ativo</div>
        <div class="destaque-valor" style="font-size:16px">{tema_top}</div>
        <div class="destaque-sub">{tema_top_n:,} proposições no período</div>
    </div>
    """, unsafe_allow_html=True)

with d3:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Taxa de efetividade</div>
        <div class="destaque-valor">{taxa_efet:.1f}%</div>
        <div class="destaque-sub">das proposições com desfecho final viraram norma</div>
    </div>
    """, unsafe_allow_html=True)

with d4:
    st.markdown(f"""
    <div class="destaque-box">
        <div class="destaque-label">Deputados envolvidos</div>
        <div class="destaque-valor">{ids_deps:,}</div>
        <div class="destaque-sub">com proposições no período</div>
    </div>
    """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# SITUAÇÃO POR TEMA (barras empilhadas)
# -----------------------------------------------------------------------------
st.markdown('<div class="secao">Situação por tema</div>', unsafe_allow_html=True)

temas_df = contagem_por_tema(df).head(20)

fig_temas = go.Figure()
col_map = {
    "Em tramitação":                  ("tramitando",  "#2563EB"),
    "Arquivada":                      ("arquivadas",  "#ff4d4d"),
    "Transformada em norma jurídica": ("normas",      "#2CE793"),
}
for sit, (col, cor) in col_map.items():
    fig_temas.add_trace(go.Bar(
        name=sit,
        y=temas_df["tema"],
        x=temas_df[col],
        orientation="h",
        marker_color=cor,
        hovertemplate=f"<b>%{{y}}</b><br>{sit}: %{{x}}<extra></extra>",
    ))

fig_temas.update_layout(
    **PLOT_LAYOUT,
    barmode="stack",
    height=520,
    xaxis=dict(showgrid=True, gridcolor="#1A2744", zeroline=False, title="Nº de proposições", color="#1A2744", tickfont=dict(color="#1A2744")),
    yaxis=dict(showgrid=False, zeroline=False, autorange="reversed", color="#1A2744", tickfont=dict(color="#1A2744")),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="left", x=0,
        font=dict(size=11, color="#1A2744"),
        bgcolor="rgba(0,0,0,0)",
    ),
)
st.plotly_chart(fig_temas, use_container_width=True)

# -----------------------------------------------------------------------------
# LINHA INFERIOR: TIPO + TEMPO DE TRAMITAÇÃO
# -----------------------------------------------------------------------------
st.markdown('<div class="secao">Composição e velocidade</div>', unsafe_allow_html=True)

col_tipo, col_tempo = st.columns([1, 2])

with col_tipo:
    tipo_cont = df["siglaTipo"].value_counts().reset_index()
    tipo_cont.columns = ["tipo", "n"]

    fig_tipo = go.Figure(go.Bar(
        x=tipo_cont["tipo"],
        y=tipo_cont["n"],
        marker_color="#1A2744",
        hovertemplate="<b>%{x}</b><br>%{y:,} proposições<extra></extra>",
    ))
    fig_tipo.update_layout(
        **PLOT_LAYOUT,
        height=300,
        title=dict(text="Por tipo", font=dict(size=12, color="#1A2744"), x=0),
        xaxis=dict(showgrid=False, zeroline=False, color="#1A2744", tickfont=dict(color="#1A2744")),
        yaxis=dict(showgrid=True, gridcolor="#1A2744", zeroline=False, tickfont=dict(color="#1A2744")),
        showlegend=False,
    )
    st.plotly_chart(fig_tipo, use_container_width=True)

with col_tempo:
    tempo_tema = tempo_tramitacao_por_tema(df).head(15)

    if not tempo_tema.empty:
        tempo_tema["mediana_anos"] = (tempo_tema["mediana_dias"] / 365).round(1)

        fig_tempo_tema = go.Figure(go.Bar(
            y=tempo_tema["tema"],
            x=tempo_tema["mediana_dias"],
            orientation="h",
            marker=dict(
                color=tempo_tema["mediana_dias"],
                colorscale=[[0, "#2D6A4F"], [0.5, "#F4B942"], [1, "#DC2626"]],
                showscale=False,
            ),
            customdata=tempo_tema[["mediana_anos", "n"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Mediana: %{x:.0f} dias (%{customdata[0]:.1f} anos)<br>"
                "Proposições finalizadas: %{customdata[1]}<extra></extra>"
            ),
        ))
        fig_tempo_tema.update_layout(
            **PLOT_LAYOUT,
            height=300,
            title=dict(
                text="Tempo mediano de tramitação por tema",
                font=dict(size=12, color="#1A2744"), x=0
            ),
            xaxis=dict(
                showgrid=True, gridcolor="#1A2744",
                zeroline=False, title="dias", color="#1A2744", tickfont=dict(color="#1A2744")
            ),
            yaxis=dict(showgrid=False, zeroline=False, autorange="reversed", color="#1A2744", tickfont=dict(color="#1A2744")),
            showlegend=False,
        )
        st.plotly_chart(fig_tempo_tema, use_container_width=True)
    else:
        st.info("Dados insuficientes para calcular tempo de tramitação com os filtros atuais.")
