# CRITICAL DATA PROTECTION WARNING — READ FIRST

**ALL existing Labelbox datasets are irreplaceable and must NEVER be modified, deleted, or altered in any way.** This applies to every dataset in the organization, not just the ones used in this project. Even though this project only uses datasets whose names start with `2024_bci`, the protection applies universally. These datasets contain years of expert botanical annotations that cannot be recreated. Any script that interacts with Labelbox must be **read-only** against existing datasets. The only write operations permitted are creating **new** datasets, projects, ontologies, and model runs.

**Hard rules:**
- NEVER call `dataset.delete()`, `data_row.delete()`, `project.delete()`, or any destructive method on existing resources
- NEVER update, overwrite, or modify existing data rows, labels, or annotations
- NEVER move data rows between existing datasets
- Only CREATE new resources (new datasets, new projects, new ontologies, new model runs)
- All export operations are read-only and safe

**Three-stage safety protocol for any script that reads existing Labelbox data:**
1. **Stage 1 — Demo dataset:** First test against the `Brazilian Amazon Trees - Demo` dataset (contains only 4 data rows). Get human sign-off before proceeding.
2. **Stage 2 — One real dataset:** Run against a single `2024_bci*` dataset. Get human sign-off before proceeding.
3. **Stage 3 — All datasets:** Run against all `2024_bci*` datasets.

Never skip stages. Never proceed without explicit human confirmation.

---

# Project: BCI Drone Photo Species Identification Workshop

## Goal

Build a hands-on workshop pipeline for a working group in Panama. The workshop integrates Pl@ntNet predictions and Pl@ntNet embeddings with Labelbox to help identify species from thousands of drone close-up photos acquired and labelled over the years on Barro Colorado Island (BCI).

## Reference Repository

GitHub repo: `https://github.com/elaliberte/labelbox_plantnet`, branch: `feature/plantnet-embeddings`

This repo contains a working pipeline for a Brazilian Amazon project. Our BCI pipeline adapts and extends this approach. All code conventions should follow patterns established in this repo.

## Key Documentation

- Labelbox SDK & API: https://docs.labelbox.com/docs/
- Pl@ntNet API: https://my.plantnet.org/doc/openapi
- GBIF Species API: https://www.gbif.org/developer/species

---

## Architecture Overview

### Two Labelbox Projects

**Project A — Benchmark & Tracking (data manager use)**
- Purpose: Import ground truth labels (from existing mask annotations), import Pl@ntNet model predictions, compute Labelbox Radio classification metrics, track model performance over time
- Ontology uses 4 annotation types sharing the same taxon list: Radio, Checklist, BBOX, Raster Segmentation
- Full ontology specs in PLAN.md (Phase 0d, 0f)

**Project B — Botanist Labelling**
- Purpose: Botanists label/correct species identifications on drone photos. Data manager uses Catalog Slices to prioritize which images get sent to botanists.
- Ontology: BBOX tool only, with nested Radio "Species"
- **CRITICAL: Model predictions must NEVER be shown to botanists in Project B.** No pre-populated annotations. This prevents AI bias in human annotations.
- Full ontology specs in PLAN.md (Phase 0i)

### Data Architecture

- All BCI images are hosted on Alliance Canada with permanent public URLs — **never re-upload images**
- Existing BCI datasets on Labelbox have names starting with `2024_bci`
- Data rows in Labelbox can only belong to one dataset at a time — to consolidate, create a **new** combined dataset with new data rows pointing to the same Alliance Canada URLs
- A data row cannot be added to a second dataset; new data rows must be created
- Similarity search (Catalog) works only within a single dataset, hence the need for consolidation

---

## GBIF Crosswalk

Ground truth annotations use GBIF backbone accepted taxon IDs. Pl@ntNet predictions return WCVP-backbone GBIF IDs. The crosswalk resolves both ID systems to canonical scientific names, then matches on name. Details in PLAN.md (Phase 0c).

## Pl@ntNet Predictions Workflow

Two prediction tracks:
- **Single-species:** We call the `/v2/identify/k-central-america` endpoint directly (1 credit/image). Already done for all 7,717 images.
- **Multi-species:** We do NOT call the survey API ourselves (rate limit constraints). Instead: export image list CSV → send to Pl@ntNet team → receive predictions JSON back. Awaiting team.

Details in PLAN.md (Phase 0h, Phase 1).

## Pl@ntNet Embeddings

Use the `/v2/embeddings` API route — one global embedding per image, one API call per image. This is separate from the predictions workflow. Details in PLAN.md (Phase 2).

## Metrics

Intended primary metric: Labelbox Radio classification metric (Model Run Radio prediction vs ground truth Radio label). However, Labelbox automatic classification metrics do not fire for this setup (investigated exhaustively with IOU=0 threshold, raw NDJSON, reduced class count — none triggered auto-metrics). Workaround: use per-image confusion matrix in Model Run UI, or compute metrics externally from exported GT/prediction data. Radio GT is pre-filled with the dominant species (most mask pixels) for each image.

---

## Key Technical Decisions Summary

1. **Dataset consolidation:** New combined dataset with new data rows pointing to same Alliance Canada URLs. Never move or copy existing data rows.
2. **Ontology size:** Project A uses 4 annotation types x N species. Must verify N x 4 <= 4,000 before creating. Include species + genus + family + empty option.
3. **Project B ontology:** Adapted from `BCI close-up photo segmentation (espanol)` with WCVP IDs replacing GBIF backbone IDs as nested classification values. Keeps genus and family levels.
4. **ID system:** Ground truth uses GBIF backbone IDs. Pl@ntNet returns WCVP IDs. Crosswalk resolves via canonical scientific names.
5. **Pl@ntNet predictions:** Single-species identify endpoint called directly (1 credit/image). Multi-species survey endpoint: export image list, send to Pl@ntNet team, receive predictions JSON back.
6. **Embeddings via `/v2/embeddings`:** One global embedding per image. Separate from predictions workflow.
7. **Botanist isolation:** Project B never shows model predictions. Data manager uses Catalog Slices to prioritize, not pre-populated labels.
8. **Image hosting:** Alliance Canada permanent URLs. Never re-upload.
9. **All existing Labelbox datasets are irreplaceable:** Read-only access only. Only create new resources.

---

See [PLAN.md](PLAN.md) for the implementation roadmap and current progress.
