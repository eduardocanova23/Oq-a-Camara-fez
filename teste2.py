import pandas as pd
df = pd.read_parquet("data/interim/proposicoes_enriquecidas.parquet")
print(f"Total: {len(df)}")
print(f"Com temas: {df['temas_ok'].sum()}")
print(f"Com autores: {df['autores_ok'].sum()}")
print(f"Sem temas: {(df['temas_tuplas_json'] == '[]').sum()}")