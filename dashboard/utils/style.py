def apply_global_style():
    import streamlit as st
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        font-feature-settings: "tnum" 1; /* números alinhados */
    }

    /* títulos e nomes */
    .dep-nome, .dep-nome-lg {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600;
    }

    /* números grandes (stats) */
    .stat-valor {
        font-family: 'Inter', sans-serif !important;
        font-weight: 700;
        font-feature-settings: "tnum" 1;
    }
    </style>
    """, unsafe_allow_html=True)