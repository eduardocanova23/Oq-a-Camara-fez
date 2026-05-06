# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**oq-a-camara-faz** ("What did Congress do?") is a Brazilian legislative analytics system. It collects proposals from the Chamber of Deputies (Câmara dos Deputados) public API since 2019, enriches and classifies them, then visualizes the data in a Streamlit dashboard.

## Commands

### Pipeline (data ETL)

```bash
cd pipeline
pip install -r requirements.txt

# Run steps in order:
python 0_deputados.py       # fetch all deputies (legislatures 56-57)
python 0_coautorias.py      # pre-compute top-5 co-authors per deputy
python 1_coletar.py         # download & filter annual proposal CSVs
python 2_enriquecer.py      # enrich via API (themes + authors), checkpointed every 250 rows
python 4_colapsar.py        # group linked proposals, elect representative
python 5_consolidar.py      # final cleanup → data/final/base_legislativa.parquet
```

Steps 3 (`3_classificar_teste*.py`) are experimental GPT-4o-mini classification scripts, not part of the main flow. Step 6 (`6_atualizar.py`) is a future Supabase push, not yet implemented.

### Dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

## Architecture

The project has two independent layers that share only the parquet files in `pipeline/data/final/`.

### Data pipeline (`pipeline/`)

Sequential numbered scripts; each reads from the previous step's output:

```
API CSVs → 1_coletar (raw/*.parquet)
         → 2_enriquecer (interim/proposicoes_enriquecidas.parquet)
         → 4_colapsar   (interim/proposicoes_colapsadas.parquet)
         → 5_consolidar (final/base_legislativa.parquet)
```

- `config.py` is the single source of truth for proposal types (`PL`, `PEC`, `PLP`, `PDL`, `PDC`), 48 theme codes (`CODTEMA_PARA_NOME`), situation status codes mapped to 3 classes (enacted / pending / archived), date range, and API base URL.
- `state/ultima_atualizacao.json` tracks which years were processed and the last run timestamp so interrupted enrichment runs can resume.
- Enrichment is the slow step (one API call per proposal); checkpointing saves every 250 rows.

### Dashboard (`dashboard/`)

Multi-page Streamlit app:

| File | Role |
|---|---|
| `app.py` | Router — registers pages via `st.navigation` |
| `app_home.py` | Landing page with hero image and navigation cards |
| `pages/1_painel_geral.py` | Macro view: proposals by theme, situation, type over time |
| `pages/2_deputados.py` | Deputy list with thematic profile cards |
| `pages/_perfil_deputado.py` | Individual deputy: themes, co-author network, proposal list |
| `utils/dados.py` | All data loading and filtering logic (1 h cache TTL) |
| `utils/style.py` | Shared CSS and Plotly theme helpers |

Data is loaded from `../pipeline/data/final/` (relative path from `dashboard/`). Filtering in `utils/dados.py` is the central place to add new filter dimensions.

### Situation color scheme

| Situation | Color |
|---|---|
| Pending (em tramitação) | `#2E75B6` |
| Archived (arquivada) | `#A6A6A6` |
| Enacted (aprovada/transformada) | `#70AD47` |

These colors are defined in `utils/style.py` and must stay consistent across all chart types.

## Key data files

| File | Description |
|---|---|
| `pipeline/data/final/base_legislativa.parquet` | ~40 k proposals, main fact table |
| `pipeline/data/final/deputados.parquet` | All deputies with party, state, photo URL |
| `pipeline/data/final/coautorias.parquet` | Top-5 co-authors per deputy (pre-computed) |

## Human rights classification (experimental)

`3_classificar_teste_v2.py` uses GPT-4o-mini with a voting system (3 independent runs, majority wins) to classify proposals under a human rights taxonomy defined in `taxonomia_direitos_humanos.docx`. Classification templates live in `gabarito_classificacao.py`. This feature is not yet integrated into the main pipeline.
