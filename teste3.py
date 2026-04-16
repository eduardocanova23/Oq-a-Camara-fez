import json
import time
import requests
import pandas as pd

df = pd.read_parquet("data/interim/proposicoes_enriquecidas.parquet")

# proposições da leg 56 (2019-2022) sem temas
df["_dt"] = pd.to_datetime(df["dataApresentacao"], errors="coerce")
df["_ano"] = df["_dt"].dt.year

def n_temas(x):
    if pd.isna(x) or x is None:
        return 0
    try:
        return len(json.loads(x))
    except Exception:
        return 0

df["_n_temas"] = df["temas_tuplas_json"].apply(n_temas)

leg56_sem_tema = df[(df["_ano"] <= 2022) & (df["_n_temas"] == 0)].copy()
leg56_com_tema = df[(df["_ano"] <= 2022) & (df["_n_temas"] > 0)].copy()

print(f"Leg 56 total:     {len(df[df['_ano'] <= 2022])}")
print(f"Leg 56 com tema:  {len(leg56_com_tema)}")
print(f"Leg 56 sem tema:  {len(leg56_sem_tema)}")

# amostra de 20 sem tema para checar na API agora
amostra = leg56_sem_tema.sample(min(20, len(leg56_sem_tema)), random_state=42)

session = requests.Session()
session.headers.update({"Accept": "application/json"})

print(f"\nVerificando {len(amostra)} IDs diretamente na API...\n")

resultados = []
for _, row in amostra.iterrows():
    pid = int(row["id"])
    url = f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{pid}/temas"
    try:
        r = session.get(url, timeout=30)
        dados = r.json().get("dados", [])
        n = len(dados)
        temas = [t.get("tema") for t in dados]
    except Exception as e:
        n = -1
        temas = [str(e)]

    resultados.append({
        "id":          pid,
        "ano":         int(row["_ano"]),
        "n_temas_api": n,
        "temas_api":   temas,
        "temas_salvo": row["temas_tuplas_json"],
    })
    time.sleep(0.1)

df_res = pd.DataFrame(resultados)
print(df_res[["id", "ano", "n_temas_api", "temas_api"]].to_string(index=False))

tem_tema_na_api = (df_res["n_temas_api"] > 0).sum()
print(f"\nDos {len(df_res)} verificados: {tem_tema_na_api} têm tema na API agora")
print(f"Continuam sem tema: {len(df_res) - tem_tema_na_api}")