# BCI Workshop Pipeline — Plan

See [CLAUDE.md](CLAUDE.md) for safety rules and architectural context.

## Current Status

Phase 0 complete (all phases including combined mask+BBOX GT import). Phase 0k (splits) complete. Phase 2 (embeddings) complete. Phase 1-single (single-species predictions via API) ready to run. Phase 1-multi (multi-species predictions from Pl@ntNet team) pending. Phase 3 (workshop) pending.

---

## Phase 0: Preparation (before Panama)

### 0a. Export all 2024_bci datasets + labels
- [x] Export annotations (masks) from all `2024_bci*` datasets to JSON + image URLs
- 65 datasets, 7,717 data rows — output in `output/00_exports/`
- Follow three-stage safety protocol: demo dataset → one real dataset → all datasets
- Export is read-only and safe

### 0b. Validate image URLs
- [x] Confirm Alliance Canada image URLs are still accessible
- All image URLs confirmed OK (images already in Labelbox with labels = URLs are live)
- 8 HTML map attachments returned 404 in spot-check — not a concern, supplementary only

### 0c. Build GBIF crosswalk CSV
- [x] Build backbone ↔ WCVP ID crosswalk
- 279 unique taxa: 279/279 matched (277 EXACT, 1 FUZZY, 1 remapped)
- Terminalia amazonica → Terminalia amazonia (WCVP spelling); Hippocrateaceae → Celastraceae (synonymised family)
- 1 VARIETY (Paullinia fibrigera) rolled up to parent species; 0 SUBSPECIES
- Output: `output/01_crosswalk/gbif_crosswalk.csv`, `crosswalk_summary.json`
- Taxa where the crosswalk fails are excluded from metric computation (scientifically defensible)

### 0d. Derive species list from annotations
- [x] Compile species list for Project A ontology
- 277 taxa from crosswalk (2 duplicates merged) + 82 genera added + 32 families added = 391 total taxon options
- Families: 53, Genera: 146, Species: 192 — plus 1 empty option = 392 options x 4 types = 1,568 (well within 4,000 limit)
- WCVP matched: 390/391 (Cordiaceae unmatched — WCVP uses Boraginaceae; excluded from metrics)
- Output: `output/02_species_list/bci_species_list.csv`, `species_list_summary.json`

### 0e. Create combined BCI dataset
- [x] Create a new combined dataset in Labelbox with new data rows pointing to the same Alliance Canada URLs
- Dataset: `BCI Workshop - Drone Photos` — 7,717 data rows
- Global keys prefixed with `comb_`; original global key stored in `original_global_key` metadata field
- Metadata fields and attachments carried over from source datasets
- Never move or copy existing data rows — created new ones

### 0f. Create Project A ontology + project
- [x] Create Project A ontology and project in Labelbox (v2)
- Project/ontology name: `BCI Workshop - All Label Types v2`
- 389 unique taxon options (391 taxa minus 2 WCVP duplicates: Hippocrateaceae=Celastraceae, Tetragastris=Protium)
- Option labels use `wcvp_canonical_name`, values use `wcvp_gbif_id`
- Ontology has 5 annotation types:
  - `[Global] Radio: "Taxon"` — dominant taxon per image (primary metric tool)
  - `[Global] Checklist: "Taxa"` — all taxa present per image
  - `[Global] Checklist: "Organs"` — Leaf / Flower / Fruit / Branch / Bark
  - `[Tool] BBOX: "Plant box"` with nested Radio "Taxon" + Checklist "Organs"
  - `[Tool] RASTER_SEGMENTATION: "Plant mask"` with nested Checklist "Taxa"
- Combined dataset (7,717 rows) sent to project
- `original_labelbox_url` string metadata added to 7,268 rows pointing back to original Labelbox data row

### 0g+0j. Import ground truth labels into Project A (combined)
- [x] Import mask GT + BBOX GT as a single combined label per image (one label layer)
- Replaces separate Phase 0g (mask-only) and Phase 0j (bbox-only) scripts
- Script: `scripts/12_import_gt_combined/12_import_gt_combined.py`
- Mask annotations: Radio "Taxon" (dominant) + Checklist "Taxa" (all) + "Plant mask" raster seg per PNG
- BBOX annotations: "Plant box" ObjectAnnotation with nested Radio "Taxon" per box
- Label resolution for BBOX: direct name match → manual override → 6+4 letter code match
- 4,866 labels imported (5,239 built; 373 individual annotation errors from source coords 1-2px out of bounds — clamped to 3999×2999 in script for future re-runs)
- 16,858 mask annotations + 19,595 BBOX boxes imported
- Input: `output/00_exports/`, `input/boxes/crop_bounding_boxes.csv`

### 0h. Export dataset JSON for Pl@ntNet team
- [x] Export image list CSV for Pl@ntNet team
- 7,717 rows: global_key, image_url, mission
- Output: `output/05_export_for_plantnet/bci_images_for_plantnet.csv`
- They will run the multi-species predictions and return a JSON with results
- We do NOT call the survey API ourselves (rate limit constraints)

### 0k. Import train/val/test split metadata into combined dataset
- [x] Import split assignments as reserved `split` enum metadata field on combined dataset data rows
- Input: `input/boxes/bci_images_for_plantnet_w_split.csv` (global_key, image_url, mission, split)
- 3,324 rows assigned (train=2,256 / valid=488 / test=580); 4,393 rows have no split (left unset)
- 467 duplicate global_keys in CSV — all consistent (same split value), deduplicated on import
- Uses reserved org-level `split` enum schema (id: `cko8sbczn0002h2dkdaxb5kal`)
- Script: `scripts/10_splits/10_import_splits.py`


### 0i. Create Project B ontology + project
- [x] Create Project B ontology and project in Labelbox
- Project/ontology name: `BCI Workshop - Botanist Labelling`
- Taxon list source: ontology `cm9fy6wm00xis073obwoa5228` (`BCI close-up photo segmentation - single list (espanol)`) — 1,931 options
- GBIF backbone IDs replaced with WCVP GBIF IDs: 1,914 EXACT, 7 FUZZY, 10 OVERRIDE (families synonymised in WCVP)
- 1,880 unique taxon options after deduplication on WCVP ID; sorted alphabetically by label
- Ontology structure:
  - `[Tool] BBOX: "Planta"` with nested:
    - Radio `"Taxón"` — species/genus/family, WCVP IDs as values
    - Checklist `"Órgano"` — Flor (flower) / Fruta (fruit)
- Crosswalk: `output/09_project_b/project_b_taxon_crosswalk.csv`
- Data rows not yet sent — will be done via Catalog Slices in Phase 3

---

## Phase 1: Pl@ntNet Predictions

Two model runs will be created in Project A for comparison:
- **Single-species** (`k-central-america` identify endpoint, 1 credit/image) — can run now
- **Multi-species** (survey tiles endpoint, sent to Pl@ntNet team) — awaiting team JSON

### 1a-single. Get single-species predictions from Pl@ntNet API
- [x] Call `/v2/identify/k-central-america` for all 7,717 images (1 credit each)
- Center-crop to 1280×1280px, same pattern as embeddings script
- Returns top-5 species + organ prediction per image; organ from `predictedOrgans[]` array
- Per-image cache → safe to stop and resume
- 7,679 cached (38 errors: API returned no species), 0 failures
- Pl@ntNet model version: `2026-02-17 (7.4)`
- Script: `scripts/13_single_predictions/13a_get_single_predictions.py`
- Output: `output/13_single_predictions/predictions.json`, per-image cache

### 1b+1c-single. Apply crosswalk and import into Project A Model Run
- [x] Resolve Pl@ntNet GBIF IDs → WCVP canonical names via crosswalk
- [x] Import into Model Run `Pl@ntNet Single - Central America` / run `v7.4-2026-03-27`:
  - [Global] Radio "Taxon" = top-1 species with confidence score
  - [Global] Checklist "Organs" = predicted organs (Leaf/Flower/Fruit/Branch/Bark)
- 3,721 / 7,679 predictions resolved (48.5% coverage — species outside ontology excluded)
- GT labels linked via `upsert_labels(project_id=...)`, splits assigned
- Model/run names in `config.yaml` under `plantnet.single_model_name` / `single_model_run_name`
- Script: `scripts/13_single_predictions/13b_import_single_predictions.py`

### 1d-single. Review metrics in Labelbox UI
- [ ] Verify Radio classification metrics appear correctly in Project A Model Run
- Data confirmed: GT + predictions present for ~39% of model run rows; feature schema IDs match

### 1a-multi. Parse multi-species predictions JSON _(awaiting Pl@ntNet team)_
- [ ] Parse and validate the multi-species predictions JSON from Pl@ntNet team
- They run `/v2/survey/tiles/k-central-america` on all 7,717 images

### 1b-multi. Apply GBIF crosswalk to multi-species predictions
- [ ] Map Pl@ntNet GBIF IDs → WCVP canonical names

### 1c-multi. Import multi-species predictions into Project A Model Run
- [ ] Import as separate Model Run `PlantNet Multi-Species (k-central-america)`
- Same annotation types: Radio "Taxon" (dominant) + Checklist "Organs"

### 1d-multi. Compare single vs multi-species metrics
- [ ] Compare Radio classification metrics between both model runs in Labelbox UI

---

## Phase 2: Embeddings (can run in parallel with Phase 1)

### 2a. Call /v2/embeddings for each image
- [x] Use the `/v2/embeddings` API route — one call per image, returns one global embedding vector
- 7,717/7,717 images processed, 0 failures
- Center-cropped to 1280×1280px before sending; 768-dimensional vectors
- Pl@ntNet model version: `2026-02-17 (7.4)`
- Output: `output/08_embeddings/embeddings.json`, per-image cache in `output/08_embeddings/cache/`

### 2b. Upload embeddings to combined dataset
- [x] Upload embedding vectors to Labelbox for the combined BCI dataset
- 7,717 vectors uploaded to custom embedding `PlantNet-v7.4-1280px` (id: `clofu0ci70702001bmn7gt2ns`)
- Uploaded in 8 batches of 1,000; ingested asynchronously by Labelbox
- Output: `output/08_embeddings/embeddings_upload.ndjson`, `upload_summary.json`

### 2c. Demo catalog similarity search
- [ ] Demonstrate similarity search working within the combined dataset in Labelbox Catalog

---

## Phase 3: Workshop — Botanist Labelling Setup

### 3a. Create Catalog Slices from Project A Model Run
- [ ] Data manager creates filtered slices based on model predictions

### 3b. Send prioritized images to Project B
- [ ] Send images to Project B for botanist labelling — NO pre-populated annotations, NO model predictions shown

### 3c. Botanists label species
- [ ] Botanists use BBOX + nested Radio to label species identifications
