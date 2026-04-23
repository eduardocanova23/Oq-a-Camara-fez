import pandas as pd
from utils.dados import carregar_base, carregar_deputados, parse_list_safe

df = carregar_base()
df = df[df["dataApresentacao"] >= pd.Timestamp("2023-02-01")].copy()
deps = carregar_deputados()

def autor_principal_id(x):
    autores = parse_list_safe(x)
    if not autores:
        return None
    validos = [a for a in autores if isinstance(a, dict)]
    if not validos:
        return None
    principal = min(validos, key=lambda a: a.get("ordemAssinatura") or 999)
    return principal.get("idDeputado")

df["_id_principal"] = pd.to_numeric(df["autores_deputados"].apply(autor_principal_id), errors="coerce")
ids_principais = df["_id_principal"].dropna().astype(int).unique()

print(f"IDs únicos de autores principais: {len(ids_principais)}")
print(f"Desses, na leg 57: {len(set(ids_principais) & set(deps[deps['idLegislatura']==57]['id'].astype(int)))}")
print(f"Desses, só na leg 56: {len(set(ids_principais) & set(deps[deps['idLegislatura']==56]['id'].astype(int)) - set(deps[deps['idLegislatura']==57]['id'].astype(int)))}")
print(f"Total de deputados na leg 57 no parquet: {len(deps[deps['idLegislatura']==57])}")