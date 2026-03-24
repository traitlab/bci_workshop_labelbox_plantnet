# BCI Workshop Pipeline — Plan

See [CLAUDE.md](CLAUDE.md) for safety rules and architectural context.

## Current Status

Phase 0 (preparation, before Panama) — in progress

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
- [ ] Compile species list for Project A ontology
- Pipeline:
  1. Export all `2024_bci*` dataset annotations (read-only)
  2. Compile frequency table of annotated taxa
  3. Filter to keep only taxa (at species, genus, and family levels) that actually appear in annotations
  4. For every species-level taxon in the ground truth, ensure the corresponding genus and family are also included (even if no genus/family-level annotations exist)
  5. Include an empty/no classification option
  6. Run GBIF crosswalk to resolve backbone ↔ WCVP ID mapping
  7. **Check ontology size:** count = number of unique options x 4 annotation types. Must be <= 4,000. If it exceeds, apply filters (minimum observation count, exclude unresolvable taxa)
  8. Output: `bci_species_list.csv`

### 0e. Create combined BCI dataset
- [ ] Create a new combined dataset in Labelbox with new data rows pointing to the same Alliance Canada URLs
- Never move or copy existing data rows — create new ones
- Required for Catalog similarity search (works only within a single dataset)

### 0f. Create Project A ontology + project
- [ ] Create Project A ontology and project in Labelbox
- Ontology has 4 annotation types sharing the same taxon list (from `bci_species_list.csv`):
  - `[Global] Radio: "Dominant species"` — primary metric tool (GT = max-pixel species from masks; prediction = max-coverage species from Pl@ntNet)
  - `[Global] Checklist: "Species present"` — presence/absence, no nested values
  - `[Tool] BBOX: "Plant prediction"` — for Pl@ntNet box predictions, with nested Radio "Species"
  - `[Tool] RASTER_SEGMENTATION: "Plant mask"` — for importing existing mask ground truth, with nested Radio "Species"

### 0g. Import ground truth labels into Project A
- [ ] Import existing mask labels + derived Radio/Checklist annotations
- Radio field must be pre-filled with the dominant species (most mask pixels) per image — required for Labelbox metrics to work

### 0h. Export dataset JSON for Pl@ntNet team
- [ ] Export combined dataset (image URLs + metadata) as JSON file to send to Pl@ntNet team
- They will run the multi-species predictions and return a JSON with results
- We do NOT call the survey API ourselves (rate limit constraints)

### 0i. Create Project B ontology
- [ ] Create Project B ontology (BBOX only, with nested Radio "Species")
- Taxon list source: existing ontology `BCI close-up photo segmentation (espanol)`
- Adaptations:
  - Replace nested classification `value` (currently GBIF backbone Accepted ID) with WCVP-backbone GBIF ID (what Pl@ntNet returns)
  - Keep genus-level and family-level entries for all species
  - This taxon list is specific to Project B and differs from Project A's list

---

## Phase 1: Pl@ntNet Predictions (after receiving JSON back)

### 1a. Parse returned predictions JSON
- [ ] Parse and validate the predictions JSON from Pl@ntNet team

### 1b. Apply GBIF crosswalk
- [ ] Map WCVP IDs in predictions → backbone IDs for Project A compatibility

### 1c. Import predictions into Project A Model Run
- [ ] Import as BBOX + nested Radio + global Radio/Checklist annotations

### 1d. Review metrics in Labelbox UI
- [ ] Verify Radio classification metrics (accuracy, per-class F1, confusion matrix) appear correctly

---

## Phase 2: Embeddings (can run in parallel with Phase 1)

### 2a. Call /v2/embeddings for each image
- [ ] Use the `/v2/embeddings` API route — one call per image, returns one global embedding vector

### 2b. Upload embeddings to combined dataset
- [ ] Upload embedding vectors to Labelbox for the combined BCI dataset

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
