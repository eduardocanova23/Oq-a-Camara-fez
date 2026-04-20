# teste_dados.py (na pasta dashboard/)
from utils.dados import carregar_base, carregar_deputados, base_com_deputados

df = carregar_base()
print(f"Base: {len(df)} grupos")
print(f"Colunas novas: _temas_lista, _autor_principal, _dias_tramitacao")
print(df[["group_id", "situacao_simplificada", "_temas_lista", "_dias_tramitacao"]].head(3))

deps = carregar_deputados()
print(f"\nDeputados: {len(deps)}")

df_enrich = base_com_deputados()
print(f"\nBase com deputados: {len(df_enrich)}")
print(f"Com partido: {df_enrich['partido_autor'].notna().sum()}")