import pandas as pd
from pathlib import Path

raw_dir = Path("data/raw")

for parquet in sorted(raw_dir.glob("*.parquet")):
    df = pd.read_parquet(parquet)
    print(f"\n{'='*50}")
    print(f"Arquivo: {parquet.name}")
    print(f"Linhas: {len(df)}")
    print(f"Colunas: {len(df.columns)}")
    print(f"\nTipos:")
    print(df["siglaTipo"].value_counts())
    print(f"\nAnos de apresentação:")
    print(pd.to_datetime(df["dataApresentacao"], errors="coerce").dt.year.value_counts().sort_index())
    print(f"\nNulos em colunas-chave:")
    for col in ["id", "siglaTipo", "dataApresentacao", "ultimoStatus_idSituacao", "ementa"]:
        n = df[col].isna().sum() if col in df.columns else "COLUNA AUSENTE"
        print(f"  {col}: {n}")