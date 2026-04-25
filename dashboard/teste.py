import json
import json
import pandas as pd

def _parse_temas(x):
    if pd.isna(x) or x is None:
        return []
    try:
        dados = json.loads(x) if isinstance(x, str) else x
        return [str(t[0]).strip() for t in dados if isinstance(t, list) and len(t) >= 1 and t[0]]
    except:
        return []

df = pd.read_parquet(r"C:\Users\Utilisateur\Desktop\oq-a-camara-faz\pipeline\data\final\base_legislativa.parquet")
df["_temas_lista"] = df["temas_tuplas_json"].apply(_parse_temas)

tema_alvo = "Direitos Humanos e Minorias"
df_tema = df[df["_temas_lista"].apply(lambda lst: tema_alvo in lst)]

# Amostra estratificada por tipo
amostra = df_tema.groupby("siglaTipo", group_keys=False).apply(
    lambda g: g.sample(min(len(g), 6), random_state=99)
).sample(frac=1, random_state=99).head(20).reset_index(drop=True)

for i, row in amostra.iterrows():
    print(f"\n[{i+1}] [{row['siglaTipo']}] {row['ementa']}")
    print(f"  keywords: {row['keywords']}")