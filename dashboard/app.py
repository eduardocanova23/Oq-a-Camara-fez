import streamlit as st

st.set_page_config(
    page_title="Câmara Data",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded",
)

pg = st.navigation({
    "": [
        st.Page("app_home.py",             title="Início",       icon="🏛️"),
        st.Page("pages/1_painel_geral.py", title="Painel Geral", icon="📊"),
        st.Page("pages/2_deputados.py",    title="Deputados",    icon="👤"),
    ],
    " ": [
        st.Page("pages/_perfil_deputado.py", title="Perfil"),
    ],
})
pg.run()