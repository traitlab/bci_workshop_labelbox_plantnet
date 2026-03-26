# BCI Drone Photo Species Identification Workshop

A pipeline for a botanical working group in Panama. Integrates Pl@ntNet predictions and embeddings with Labelbox to help identify species from drone close-up photos acquired on Barro Colorado Island (BCI).

## Overview

The pipeline has two Labelbox projects:

- **Project A — Benchmark & Tracking:** Imports existing ground truth mask labels and Pl@ntNet model predictions. Computes Radio classification metrics to benchmark model performance.
- **Project B — Botanist Labelling:** Botanists label/correct species identifications. Model predictions are never shown to avoid AI bias.

See [PLAN.md](PLAN.md) for the full implementation roadmap and current progress.

## Setup

**Prerequisites:** Python 3.10+

```bash
python -m venv .venv
.venv\Scripts\activate       # Windows
# source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

**API keys:** Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

**Verify setup:**

```bash
python test_setup.py
```

## Pipeline

Scripts are organized by phase under `scripts/`. Run them in order. All output goes to `output/` (git-ignored).

### Phase 0a — Export existing datasets

```bash
# Stage 1: demo dataset only (4 rows) — review before proceeding
python scripts/00_export/00_export_datasets.py --stage 1

# Stage 2: one real dataset — review before proceeding
python scripts/00_export/00_export_datasets.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all 2024_bci* datasets
python scripts/00_export/00_export_datasets.py --stage 3
```

> **Data safety:** All existing Labelbox datasets are irreplaceable. The export script is strictly read-only. See [CLAUDE.md](CLAUDE.md) for the full safety protocol.

### Phase 0b — Validate image URLs

```bash
# Sample check (5 rows per dataset)
python scripts/01_validate_urls/01_validate_urls.py

# Full check (all rows)
python scripts/01_validate_urls/01_validate_urls.py --all
```

### Phase 0c — Build GBIF crosswalk

```bash
python scripts/02_crosswalk/02_build_crosswalk.py
```

Resolves GBIF backbone taxon IDs (used in ground truth labels) to WCVP-backbone IDs (used by Pl@ntNet predictions). Output: `output/01_crosswalk/gbif_crosswalk.csv`. API responses are cached in `output/01_crosswalk/gbif_api_cache.json` so re-runs are fast.

### Phase 0d — Build species list

```bash
python scripts/03_species_list/03_build_species_list.py
```

Derives the Project A ontology taxon list from the crosswalk. Ensures every annotated species has its parent genus and family present. Checks the 4,000-option Labelbox ontology limit. Output: `output/02_species_list/bci_species_list.csv`.

### Phase 0e — Create combined dataset

```bash
# Stage 1: 3 demo rows — review before proceeding
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 1

# Stage 2: one dataset — review before proceeding
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all datasets
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 3
```

Creates the `BCI Workshop - Drone Photos` Labelbox dataset with new data rows pointing to the same Alliance Canada URLs. Global keys are prefixed with `comb_`; the original global key is stored in an `original_global_key` metadata field. Idempotent: skips already-uploaded rows on re-runs.

### Phase 0h — Export image list for Pl@ntNet team

```bash
# Test: first 2 rows only
python scripts/05_export_for_plantnet/05_export_for_plantnet.py --test

# Full export: all 7,717 rows
python scripts/05_export_for_plantnet/05_export_for_plantnet.py
```

Exports a CSV (`global_key`, `image_url`, `mission`) for the Pl@ntNet team to run multi-species survey predictions. Output: `output/05_export_for_plantnet/bci_images_for_plantnet.csv`.

### Phase 2a — Get Pl@ntNet embeddings

```bash
# Test: 1 image, verbose output
python scripts/08_embeddings/08_get_embeddings.py --test

# Full run: all 7,717 images (~65 min at default 0.5s delay)
python scripts/08_embeddings/08_get_embeddings.py

# Custom delay
python scripts/08_embeddings/08_get_embeddings.py --delay 1.0
```

Downloads each image from Alliance Canada, center-crops to 1280×1280px, and calls the Pl@ntNet `/v2/embeddings` API. Each image's embedding is cached to disk immediately — safe to stop and resume. Output: `output/08_embeddings/embeddings.json` (7,717 × 768-dimensional vectors).

### Phase 2b — Upload embeddings to Labelbox

```bash
# Test: 5 rows only
python scripts/08_embeddings/08b_upload_embeddings.py --test

# Full upload
python scripts/08_embeddings/08b_upload_embeddings.py
```

Maps embeddings to Labelbox data row IDs, writes an NDJSON file, and uploads to a custom Labelbox embedding (`PlantNet-v7.4-1280px`). Creates the embedding object if it doesn't exist. Vectors are ingested asynchronously by Labelbox — similarity search in Catalog activates once all vectors are indexed (requires ≥1,000).

## Configuration

All pipeline settings are in [config.yaml](config.yaml):

- `labelbox.dataset_prefix` — prefix for BCI datasets to export
- `labelbox.label_projects` — project names from which to keep labels (demo projects excluded)
- `labelbox.combined_dataset_name` — name for the new consolidated dataset (Phase 0e)
- `plantnet.embeddings_api_url` — Pl@ntNet `/v2/embeddings` endpoint
- `folders.*` — output paths for each pipeline stage

## Reference

- Reference repo: [elaliberte/labelbox_plantnet](https://github.com/elaliberte/labelbox_plantnet) (branch: `feature/plantnet-embeddings`)
- Labelbox SDK docs: https://docs.labelbox.com/docs/
- Pl@ntNet API: https://my.plantnet.org/doc/openapi
- GBIF Species API: https://www.gbif.org/developer/species
