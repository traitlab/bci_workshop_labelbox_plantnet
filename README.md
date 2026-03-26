# 🌿 BCI Drone Photo Species Identification Workshop

A pipeline for a botanical working group in Panama. Integrates Pl@ntNet predictions and embeddings with Labelbox to help identify species from thousands of drone close-up photos acquired on Barro Colorado Island (BCI).

## 🗺️ Overview

The pipeline has two Labelbox projects:

- **Project A — Benchmark & Tracking:** Imports existing ground truth mask labels and Pl@ntNet model predictions. Computes Radio classification metrics to benchmark model performance.
- **Project B — Botanist Labelling:** Botanists label/correct species identifications. Model predictions are **never** shown to avoid AI bias.

## ✅ Status

| Phase | Description | Status |
|-------|-------------|--------|
| **0a** | Export all `2024_bci*` datasets + labels | ✅ Done |
| **0b** | Validate Alliance Canada image URLs | ✅ Done |
| **0c** | Build GBIF ↔ WCVP crosswalk | ✅ Done |
| **0d** | Derive species list for Project A ontology | ✅ Done |
| **0e** | Create combined BCI dataset (7,717 rows) | ✅ Done |
| **0f** | Create Project A ontology + project | ✅ Done |
| **0g** | Import mask GT labels into Project A (Radio, Checklist, Raster Seg) | ✅ Done |
| **0h** | Export image list for Pl@ntNet team | ✅ Done |
| **0i** | Create Project B ontology + project | ✅ Done |
| **0j** | Import BBOX ground truth labels into Project A | ⏳ Awaiting collaborator |
| **1a** | Parse Pl@ntNet predictions JSON | ⏳ Awaiting Pl@ntNet team |
| **1b** | Apply GBIF ↔ WCVP crosswalk to predictions | ⏳ Pending |
| **1c** | Import predictions into Project A Model Run | ⏳ Pending |
| **1d** | Import train/val/test data row splits | ⏳ Awaiting collaborator |
| **1e** | Review metrics in Labelbox UI | ⏳ Pending |
| **2a** | Get Pl@ntNet embeddings for all 7,717 images | ✅ Done |
| **2b** | Upload embeddings to Labelbox (similarity search) | ✅ Done |
| **2c** | Demo Catalog similarity search | ⏳ Pending |
| **3a–3c** | Workshop — Catalog Slices + botanist labelling | ⏳ Pending |

See [PLAN.md](PLAN.md) for full details on each phase.

---

## 🛠️ Setup

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

---

## 🔬 Pipeline

Scripts are organized by phase under `scripts/`. Run them in order. All output goes to `output/` (git-ignored).

### ✅ Phase 0a — Export existing datasets

```bash
# Stage 1: demo dataset only (4 rows) — review before proceeding
python scripts/00_export/00_export_datasets.py --stage 1

# Stage 2: one real dataset — review before proceeding
python scripts/00_export/00_export_datasets.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all 2024_bci* datasets
python scripts/00_export/00_export_datasets.py --stage 3
```

> ⚠️ **Data safety:** All existing Labelbox datasets are irreplaceable. The export script is strictly read-only. See [CLAUDE.md](CLAUDE.md) for the full safety protocol.

### ✅ Phase 0b — Validate image URLs

```bash
# Sample check (5 rows per dataset)
python scripts/01_validate_urls/01_validate_urls.py

# Full check (all rows)
python scripts/01_validate_urls/01_validate_urls.py --all
```

### ✅ Phase 0c — Build GBIF crosswalk

```bash
python scripts/02_crosswalk/02_build_crosswalk.py
```

Resolves GBIF backbone taxon IDs (used in ground truth labels) to WCVP-backbone IDs (used by Pl@ntNet predictions). Output: `output/01_crosswalk/gbif_crosswalk.csv`. API responses are cached so re-runs are fast.

### ✅ Phase 0d — Build species list

```bash
python scripts/03_species_list/03_build_species_list.py
```

Derives the Project A ontology taxon list from the crosswalk. Ensures every annotated species has its parent genus and family present. Checks the 4,000-option Labelbox ontology limit. Output: `output/02_species_list/bci_species_list.csv`.

### ✅ Phase 0e — Create combined dataset

```bash
# Stage 1: 3 demo rows — review before proceeding
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 1

# Stage 2: one dataset — review before proceeding
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all datasets
python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 3
```

Creates the `BCI Workshop - Drone Photos` Labelbox dataset with new data rows pointing to the same Alliance Canada URLs. Global keys are prefixed with `comb_`; the original global key is stored in an `original_global_key` metadata field. Idempotent: skips already-uploaded rows on re-runs.

### ✅ Phase 0f — Create Project A

```bash
python scripts/06_project_a/06_create_project_a.py
```

Creates the `BCI Workshop - All Label Types v2` ontology and project in Labelbox. Ontology has 5 annotation types (Global Radio, Global Checklist Taxa, Global Checklist Organs, BBOX, Raster Segmentation) sharing 389 taxon options.

### ✅ Phase 0g — Import ground truth labels

```bash
# Stage 1: demo dataset — review before proceeding
python scripts/07_import_gt/07_import_ground_truth.py --stage 1

# Stage 2: one dataset — review before proceeding
python scripts/07_import_gt/07_import_ground_truth.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all datasets
python scripts/07_import_gt/07_import_ground_truth.py --stage 3
```

Downloads each mask PNG, counts alpha pixels to find the dominant taxon, and imports Radio + Checklist + Raster Segmentation labels into Project A. Uses 30 parallel download workers with disk caching.

### ✅ Phase 0h — Export image list for Pl@ntNet team

```bash
# Test: first 2 rows only
python scripts/05_export_for_plantnet/05_export_for_plantnet.py --test

# Full export: all 7,717 rows
python scripts/05_export_for_plantnet/05_export_for_plantnet.py
```

Exports a CSV (`global_key`, `image_url`, `mission`) for the Pl@ntNet team to run multi-species survey predictions. Output: `output/05_export_for_plantnet/bci_images_for_plantnet.csv`.

### ✅ Phase 2a — Get Pl@ntNet embeddings

```bash
# Test: 1 image, verbose output
python scripts/08_embeddings/08_get_embeddings.py --test

# Full run: all 7,717 images (~65 min at default 0.5s delay)
python scripts/08_embeddings/08_get_embeddings.py

# Custom delay
python scripts/08_embeddings/08_get_embeddings.py --delay 1.0
```

Downloads each image from Alliance Canada, center-crops to 1280×1280px, and calls the Pl@ntNet `/v2/embeddings` API. Each image's embedding is cached to disk immediately — safe to stop and resume at any time. Output: `output/08_embeddings/embeddings.json` (7,717 × 768-dimensional vectors, model `v7.4`).

### ✅ Phase 2b — Upload embeddings to Labelbox

```bash
# Test: 5 rows only
python scripts/08_embeddings/08b_upload_embeddings.py --test

# Full upload
python scripts/08_embeddings/08b_upload_embeddings.py
```

Maps embeddings to Labelbox data row IDs, writes an NDJSON file, and uploads to a custom Labelbox embedding (`PlantNet-v7.4-1280px`). Creates the embedding object if it doesn't exist. Vectors are ingested asynchronously — similarity search in Catalog activates once all vectors are indexed (requires ≥1,000).

### ⏳ Phase 0j — Import BBOX ground truth labels _(awaiting collaborator)_

A collaborator is computing tight bounding boxes from the existing masks using an iterative largest-interior-rectangle algorithm. Once the file arrives:

- Import as `[Tool] BBOX: "Plant box"` annotations with nested Radio "Taxon" into Project A
- Script to be written once the input format is known

### ⏳ Phase 1 — Pl@ntNet Predictions + Splits _(pending)_

Waiting for two deliverables:
- **Pl@ntNet team** — multi-species predictions JSON
- **Collaborator** — train/val/test split assignments per data row

Once received:

1. **1a** — Parse and validate the predictions JSON
2. **1b** — Apply GBIF ↔ WCVP crosswalk to align taxon IDs
3. **1c** — Import predictions into Project A as a Model Run
4. **1d** — Import train/val/test splits as a `split` metadata field on the combined dataset
5. **1e** — Review Radio classification metrics in Labelbox UI

### ⏳ Phase 3 — Workshop: Botanist Labelling _(pending)_

Once predictions and embeddings are in place:

1. **3a** — Data manager creates Catalog Slices from Project A Model Run (prioritize uncertain/interesting images)
2. **3b** — Send prioritized images to Project B — **no model predictions shown** to botanists
3. **3c** — Botanists use BBOX + nested Radio to label species identifications

---

## ⚙️ Configuration

All pipeline settings are in [config.yaml](config.yaml):

- `labelbox.dataset_prefix` — prefix for BCI datasets to export
- `labelbox.label_projects` — project names from which to keep labels (demo projects excluded)
- `labelbox.combined_dataset_name` — name for the new consolidated dataset (`BCI Workshop - Drone Photos`)
- `plantnet.embeddings_api_url` — Pl@ntNet `/v2/embeddings` endpoint
- `folders.*` — output paths for each pipeline stage

---

## 📚 Reference

- Reference repo: [elaliberte/labelbox_plantnet](https://github.com/elaliberte/labelbox_plantnet) (branch: `feature/plantnet-embeddings`)
- Labelbox SDK docs: https://docs.labelbox.com/docs/
- Pl@ntNet API: https://my.plantnet.org/doc/openapi
- GBIF Species API: https://www.gbif.org/developer/species
