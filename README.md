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
| **0g+0j** | Import mask + BBOX GT labels into Project A (combined, single layer) | ✅ Done |
| **0h** | Export image list for Pl@ntNet team | ✅ Done |
| **0i** | Create Project B ontology + project | ✅ Done |
| **0k** | Import train/val/test split metadata into combined dataset | ✅ Done |
| **1a-single** | Get single-species predictions (`k-central-america`, 1 credit/image) | 🟡 Ready to run |
| **1b+1c-single** | Crosswalk + import into Project A Model Run (Radio Taxon + Organs) | ⏳ Pending |
| **1d-single** | Review single-species metrics in Labelbox UI | ⏳ Pending |
| **1a-multi** | Parse multi-species predictions JSON from Pl@ntNet team | ⏳ Awaiting team |
| **1b+1c-multi** | Crosswalk + import multi-species Model Run | ⏳ Pending |
| **1d-multi** | Compare single vs multi-species metrics | ⏳ Pending |
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

### ✅ Phase 0g+0j — Import ground truth labels (combined)

```bash
# Stage 1: 3 images — review before proceeding
python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 1

# Stage 2: one dataset — review before proceeding
python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 2 --dataset "2024_bci_XXXX"

# Stage 3: all datasets (deletes existing labels first, then reimports)
python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 3 --confirm-delete
```

Imports mask GT and BBOX GT as a **single combined label per image** so all annotations coexist on one layer in Project A. Downloads mask PNGs (cached), counts pixels to find the dominant taxon, and resolves BBOX labels via name/code matching. 4,866 labels imported: 16,858 mask annotations + 19,595 BBOX boxes. Stage 3 deletes all existing Project A labels before reimporting — `--confirm-delete` is required as a safety gate.

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

### ✅ Phase 0k — Import train/val/test split metadata

```bash
# Test: 5 rows only
python scripts/10_splits/10_import_splits.py --test

# Full run
python scripts/10_splits/10_import_splits.py
```

Reads `input/boxes/bci_images_for_plantnet_w_split.csv` and upserts the reserved `split` enum metadata field on each data row in the combined dataset. 3,324 rows assigned (train=2,256 / valid=488 / test=580); 4,393 rows with no split are left unset. Deduplicates on `global_key` automatically.

### 🟡 Phase 1-single — Pl@ntNet Single-Species Predictions

Calls the Pl@ntNet `/v2/identify/k-central-america` endpoint directly (1 API credit per image). Returns top-5 species + organ prediction per image. Per-image cache makes it safe to stop and resume.

```bash
# Test: 1 image, verbose output
python scripts/13_single_predictions/13a_get_single_predictions.py --test

# Full run: all 7,717 images (~1 hr at default 0.5s delay)
python scripts/13_single_predictions/13a_get_single_predictions.py

# Custom delay
python scripts/13_single_predictions/13a_get_single_predictions.py --delay 1.0
```

Then import into Project A as a Model Run (`PlantNet Single-Species (k-central-america)`):

```bash
# Test: 5 predictions only
python scripts/13_single_predictions/13b_import_single_predictions.py --test

# Full import
python scripts/13_single_predictions/13b_import_single_predictions.py
```

Imports Radio "Taxon" (top-1 species with confidence) and Checklist "Organs" annotations. Results are visible in Project A's Model Runs tab for classification metric comparison against ground truth.

### ⏳ Phase 1-multi — Pl@ntNet Multi-Species Predictions _(awaiting Pl@ntNet team)_

The Pl@ntNet team runs the survey tiles endpoint on all 7,717 images and returns a predictions JSON. This will be imported as a separate Model Run (`PlantNet Multi-Species (k-central-america)`) for side-by-side comparison with the single-species run.

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
