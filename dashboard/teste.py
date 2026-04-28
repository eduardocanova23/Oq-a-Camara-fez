import json
import time
import pandas as pd
from collections import Counter
from utils.dados import carregar_base, parse_list_safe

start = time.time()
df = carregar_base()

# só proposições com mais de um autor deputado
def extrair_ids_deputados(x):
    autores = parse_list_safe(x)
    return [
        int(a["idDeputado"]) for a in autores
        if isinstance(a, dict) and a.get("idDeputado") is not None
    ]

df["_ids_deps"] = df["autores_json"].apply(extrair_ids_deputados)
multi = df[df["_ids_deps"].apply(len) > 1].copy()

print(f"Proposições com múltiplos autores deputados: {len(multi)}")

# para cada par de deputados que coautoraram, conta quantas vezes
coautorias = Counter()
for ids in multi["_ids_deps"]:
    ids = list(set(ids))  # remove duplicatas dentro da mesma proposição
    for i in range(len(ids)):
        for j in range(i+1, len(ids)):
            par = tuple(sorted([ids[i], ids[j]]))
            coautorias[par] += 1

print(f"Pares únicos de coautoria: {len(coautorias)}")
print(f"Tempo: {time.time()-start:.1f}s")
print(f"\nTop 5 pares mais frequentes:")
for par, n in coautorias.most_common(5):
    print(f"  {par}: {n} proposições juntos")