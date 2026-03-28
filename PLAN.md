# BCI Workshop Pipeline — Plan

See [CLAUDE.md](CLAUDE.md) for safety rules and architectural context.

## Current Status

Phase 0 complete (all phases including combined mask+BBOX GT import). Phase 0k (splits) complete. Phase 1-single complete (predictions obtained + imported as Model Run; auto-metrics unavailable). Phase 2 (embeddings) complete. Phase 1-multi complete — ontology updated (Cover % added), Project A model run imported (7,563/7,717, 21,556 boxes), Project B model run imported (7,205/7,717, 38,637 boxes), multi-embeddings uploaded (7,691 vectors, PlantNet-v7.4-multi). Phase 3 (workshop) pending.

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
- [~] Labelbox automatic classification metrics not available for this setup (investigated exhaustively: IOU=0, raw NDJSON, few classes — all tested, no auto-metrics)
- Workaround: use the confusion matrix view per-image in the model run UI, or compute metrics externally from the exported GT/prediction data

### 1-multi-a. Update Project A ontology
- [x] Added nested Text "Cover (%)" to every option of global Radio "Taxon" and Checklist "Taxa"
- [x] Removed global Checklist "Organs" (organs now live only inside BBOX "Plant box")
- New ontology uid: `cmnas2u3j0ne1073w6uil18mk`
- Script: `scripts/14_multi_predictions/14a_update_ontology.py`

### 1-multi-b. Import multi-species predictions into Project A Model Run
- [x] Parsed 7,717 survey JSON files at `C:\data\plantnet\2026-02-17\`
- [x] Resolved GBIF IDs → WCVP canonical names via crosswalk (389 Project A taxa)
- [x] Imported as Model Run `Pl@ntNet Multi - Central America` / `v7.4-2026-03-28`:
  - [Global] Radio "Taxon" = highest-coverage resolved species + nested Cover (%)
  - [Global] Checklist "Taxa" = all resolved species, each with nested Cover (%)
  - [Tool] BBOX "Plant box" = one box per species (best tile, score >= 0.05) + nested Taxon + Organs
- 7,563/7,717 uploaded (98.0%), 21,556 boxes, 0 errors; 154 images had no resolved species in 389-taxon list
- 1 malformed JSON file skipped (truncated)
- Note: confidence not set on annotations — Labelbox requires consistent confidence across all leaf nodes; incompatible with nested Text (Cover %) which has no confidence
- Script: `scripts/14_multi_predictions/14b_import_multi_predictions_a.py`

### 1-multi-c. Import BBOX predictions into Project B Model Run
- [x] Imported as Model Run `Pl@ntNet Multi - Botanist` / `v7.4-2026-03-28` (1,880 taxa full set)
- BBOX "Planta" with nested Radio "Taxón" + Checklist "Órgano" (Flor/Fruta only)
- 7,503/7,717 uploaded (97.2%), 38,637 boxes, 0 errors; 214 images had no resolved species in 1,880-taxon list
- Fixed: crosswalk loading now replicates ontology deduplication (sort by label, keep first per wcvp_id)
- Script: `scripts/14_multi_predictions/14c_import_multi_predictions_b.py`

### 1-multi-d. Upload multi-species embeddings
- [x] Mean-pooled per_tiles_embeddings (768-dim) across all tiles per image, L2-normalized
- [x] Uploaded 7,691 vectors as custom embedding `PlantNet-v7.4-multi` (id: `ct6uqcrdy070001v8mnasratn`)
- Script: `scripts/14_multi_predictions/14d_upload_multi_embeddings.py`

### 1-multi-e. Compare single vs multi-species metrics
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
