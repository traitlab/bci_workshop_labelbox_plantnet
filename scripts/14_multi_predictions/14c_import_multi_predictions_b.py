"""
Phase 1-multi step c — Import multi-species BBOX predictions into Project B Model Run.

Reads 7,717 multi-species JSON files, resolves GBIF IDs to Project B's
taxon list (1,880 options, full set), and imports as BBOX predictions in a
Model Run (data manager only — never shown to botanists).

Project B ontology:
  - [Tool] BBOX: "Planta"
    - Nested Radio: "Taxón"    — label from Project B crosswalk, value = wcvp_gbif_id
    - Nested Checklist: "Órgano" — Flor / Fruta only

Model and run names from config.yaml:
  plantnet.multi_model_b_name
  plantnet.multi_model_run_b_name

Usage:
  python scripts/14_multi_predictions/14c_import_multi_predictions_b.py --test
  python scripts/14_multi_predictions/14c_import_multi_predictions_b.py
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import labelbox as lb
import labelbox.types as lb_types
from dotenv import load_dotenv
import yaml

PROJECT_B_NAME = "BCI Workshop - Botanist Labelling"

BATCH_SIZE = 100
CONFIDENCE_THRESHOLD = 0.05

# Ontology field names (Spanish, Project B)
CLS_TAXON  = "Taxón"
CLS_ORGANO = "Órgano"
TOOL_BBOX  = "Planta"

# Only flower/fruit have Project B organ options (leaf/branch/bark have no Órgano equivalent)
ORGAN_MAP_B = {
    "flower": "Flor",
    "bloom":  "Flor",
    "fruit":  "Fruta",
    "berry":  "Fruta",
}

IMG_W, IMG_H = 4000, 3000


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_project_b_crosswalk(crosswalk_path: Path) -> tuple:
    """
    Load Project B taxon crosswalk.
    Replicates the deduplication logic of 09_create_project_b.py:
      - Sort rows by label alphabetically
      - Keep the first label seen per wcvp_gbif_id (or gbif_backbone_id fallback)
    This ensures wcvp_to_label maps to the same label string that appears in the ontology.

    Returns:
      gbif_to_wcvp  : {gbif_backbone_id -> value}   where value = wcvp_gbif_id or gbif_backbone_id
      wcvp_to_label : {value -> label}              the label that is in the ontology
      valid_values  : set of values present in the ontology
    """
    # Load all rows first
    all_rows = []
    with open(crosswalk_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            gbif_id = row.get("gbif_backbone_id", "")
            wcvp_id = row.get("wcvp_gbif_id", "")
            label = row.get("label", "")
            value = wcvp_id or gbif_id
            if value:
                all_rows.append({"gbif_id": gbif_id, "value": value, "label": label})

    # Replicate build_ontology deduplication: sort by label, keep first per value
    seen_values = set()
    wcvp_to_label = {}
    for row in sorted(all_rows, key=lambda r: r["label"]):
        if row["value"] not in seen_values:
            seen_values.add(row["value"])
            wcvp_to_label[row["value"]] = row["label"]

    # Build gbif_backbone_id -> value mapping (last row wins for duplicates,
    # but value is the same for all rows with the same gbif_id)
    gbif_to_wcvp = {}
    for row in all_rows:
        if row["gbif_id"]:
            gbif_to_wcvp[row["gbif_id"]] = row["value"]

    valid_values = set(wcvp_to_label.keys())
    return gbif_to_wcvp, wcvp_to_label, valid_values


def resolve_species_b(gbif_id: str, binomial: str,
                      gbif_to_wcvp: dict, wcvp_to_label: dict,
                      valid_values: set) -> tuple:
    """
    Resolve to Project B label.
    Returns (label, value, method) or (None, None, reason).
    """
    gbif_id = str(gbif_id or "")

    # Direct GBIF ID match
    if gbif_id and gbif_id in gbif_to_wcvp:
        value = gbif_to_wcvp[gbif_id]
        if value in valid_values:
            label = wcvp_to_label.get(value)
            if label:
                return label, value, "gbif"

    # Try GBIF ID directly as a value (some IDs may already be WCVP)
    if gbif_id and gbif_id in valid_values:
        label = wcvp_to_label.get(gbif_id)
        if label:
            return label, gbif_id, "direct"

    return None, None, f"unresolved (gbif={gbif_id}, name={binomial})"


def parse_multi_json(json_path: Path) -> dict:
    """Parse a single multi-species JSON file."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results", {})
    image = results.get("image", {})

    species_list = []
    for sp in results.get("species", []):
        species_list.append({
            "gbif_id": str(sp.get("gbif_id", "")),
            "binomial": sp.get("binomial", ""),
            "tiles": sp.get("location", []),
        })

    return {
        "width": image.get("width", IMG_W),
        "height": image.get("height", IMG_H),
        "species": species_list,
    }


def build_label_b(global_key: str, parsed: dict,
                  gbif_to_wcvp: dict, wcvp_to_label: dict,
                  valid_values: set) -> tuple:
    """Build a lb_types.Label for Project B BBOX predictions."""
    combined_gk = "comb_" + global_key
    img_w = parsed["width"]
    img_h = parsed["height"]

    annotations = []
    n_resolved = 0
    n_unresolved = 0

    for sp in parsed["species"]:
        label_name, value, method = resolve_species_b(
            sp["gbif_id"], sp["binomial"],
            gbif_to_wcvp, wcvp_to_label, valid_values
        )
        if label_name is None:
            n_unresolved += 1
            continue

        # Find best tile above threshold
        best_tile = None
        for tile in sp["tiles"]:
            score = tile.get("score", 0)
            if score < CONFIDENCE_THRESHOLD:
                continue
            if best_tile is None or score > best_tile.get("score", 0):
                best_tile = tile

        if best_tile is None:
            continue

        n_resolved += 1
        cx = best_tile["center"]["x"]
        cy = best_tile["center"]["y"]
        size = best_tile["size"]

        x_min = max(0, min(cx - size / 2, img_w - 1))
        y_min = max(0, min(cy - size / 2, img_h - 1))
        x_max = max(1, min(cx + size / 2, img_w))
        y_max = max(1, min(cy + size / 2, img_h))

        if x_max <= x_min:
            x_max = x_min + 1
        if y_max <= y_min:
            y_max = y_min + 1

        # Nested Radio "Taxón" (no confidence — Labelbox requires consistent confidence
        # across all leaf nodes; mixed annotation trees can't satisfy this)
        taxon_cls = lb_types.ClassificationAnnotation(
            name=CLS_TAXON,
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(
                    name=label_name,
                )
            ),
        )

        box_classifications = [taxon_cls]

        # Nested Checklist "Órgano" — only Flor/Fruta
        organ_str = best_tile.get("organ", "")
        lb_organ = ORGAN_MAP_B.get(organ_str.lower(), "")
        if lb_organ:
            box_classifications.append(
                lb_types.ClassificationAnnotation(
                    name=CLS_ORGANO,
                    value=lb_types.Checklist(
                        answer=[lb_types.ClassificationAnswer(name=lb_organ)]
                    ),
                )
            )

        bbox = lb_types.ObjectAnnotation(
            name=TOOL_BBOX,
            value=lb_types.Rectangle(
                start=lb_types.Point(x=x_min, y=y_min),
                end=lb_types.Point(x=x_max, y=y_max),
            ),
            classifications=box_classifications,
        )
        annotations.append(bbox)

    if not annotations:
        return None, {"status": "no_boxes", "unresolved": n_unresolved}

    label = lb_types.Label(
        data={"global_key": combined_gk},
        annotations=annotations,
    )
    return label, {
        "status": "ok",
        "n_boxes": len(annotations),
        "n_resolved": n_resolved,
        "n_unresolved": n_unresolved,
    }


def get_or_create_model_run(client: lb.Client, project: lb.Project,
                            model_name: str, run_name: str) -> lb.ModelRun:
    """Find or create Model and ModelRun by name."""
    model = next((m for m in client.get_models() if m.name == model_name), None)
    if model is None:
        print(f"  Creating model '{model_name}' ...")
        model = client.create_model(
            name=model_name,
            ontology_id=project.ontology().uid,
        )
    else:
        print(f"  Found model '{model_name}' ({model.uid})")

    run = next((r for r in model.model_runs() if r.name == run_name), None)
    if run is None:
        print(f"  Creating model run '{run_name}' ...")
        run = model.create_model_run(run_name)
    else:
        print(f"  Found model run '{run_name}' ({run.uid})")

    return run


def main():
    parser = argparse.ArgumentParser(
        description="Import multi-species BBOX predictions into Project B Model Run."
    )
    parser.add_argument("--test", action="store_true",
                        help="Process first 5 files only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    crosswalk_path = Path("output/09_project_b/project_b_taxon_crosswalk.csv")
    output_dir = Path(config["folders"]["multi_predictions"])
    output_dir.mkdir(parents=True, exist_ok=True)

    MODEL_NAME = config["plantnet"]["multi_model_b_name"]
    MODEL_RUN_NAME = config["plantnet"]["multi_model_run_b_name"]
    multi_dir = Path(config["plantnet"]["multi_predictions_dir"])

    # Step 1: Load Project B crosswalk
    print("Step 1 — Loading Project B crosswalk ...")
    gbif_to_wcvp, wcvp_to_label, valid_values = load_project_b_crosswalk(crosswalk_path)
    print(f"  {len(gbif_to_wcvp)} GBIF mappings, {len(valid_values)} valid Project B taxa")

    # Step 2: List prediction files
    print("\nStep 2 — Scanning prediction files ...")
    json_files = sorted(multi_dir.glob("*.JPG.json"))
    print(f"  Found {len(json_files)} JSON files")

    if args.test:
        json_files = json_files[:5]
        print("  TEST MODE: 5 files only")

    # Step 3: Connect to Labelbox
    print("\nStep 3 — Connecting to Labelbox ...")
    client = lb.Client(api_key=api_key, enable_experimental=True)
    project = next((p for p in client.get_projects() if p.name == PROJECT_B_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_B_NAME}' not found.")
    print(f"  Project B confirmed: '{project.name}' ({project.uid})")

    # Step 4: Set up Model Run
    print("\nStep 4 — Setting up Model Run ...")
    model_run = get_or_create_model_run(client, project, MODEL_NAME, MODEL_RUN_NAME)
    print(f"  Model run ready: {model_run.uid}")

    # Step 5: Build labels
    print("\nStep 5 — Building prediction labels ...")
    labels = []
    resolved_gks = []
    total_boxes = 0
    no_boxes = 0

    for jf in json_files:
        global_key = jf.name.replace(".json", "")
        try:
            parsed = parse_multi_json(jf)
        except Exception as e:
            print(f"  WARNING: Skipping {jf.name} — parse error: {e}")
            no_boxes += 1
            continue
        label, stats = build_label_b(
            global_key, parsed,
            gbif_to_wcvp, wcvp_to_label, valid_values
        )

        if label is None:
            no_boxes += 1
        else:
            labels.append(label)
            resolved_gks.append("comb_" + global_key)
            total_boxes += stats["n_boxes"]

    print(f"  Labels built:   {len(labels)}")
    print(f"  No boxes:       {no_boxes}")
    print(f"  Total boxes:    {total_boxes}")

    if not labels:
        sys.exit("No labels to import.")

    # Step 6: Register data rows
    print(f"\nStep 6 — Registering {len(resolved_gks)} data rows ...")
    model_run.upsert_data_rows(global_keys=resolved_gks)
    print(f"  Registered {len(resolved_gks)} data rows.")

    # Step 7: Upload predictions
    print(f"\nStep 7 — Uploading {len(labels)} predictions in batches of {BATCH_SIZE} ...")
    run_ts = int(time.time())
    total_ok = total_err = 0

    for i in range(0, len(labels), BATCH_SIZE):
        batch = labels[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        job = model_run.add_predictions(
            name=f"multi_b_pred_{run_ts}_b{batch_num}",
            predictions=batch,
        )
        job.wait_till_done()
        errs = job.errors
        ok = len(batch) - len(errs)
        total_ok += ok
        total_err += len(errs)
        print(f"  Batch {batch_num}: {ok} ok, {len(errs)} errors "
              f"({total_ok}/{len(labels)} total)")
        if errs:
            for e in errs[:3]:
                print(f"    ERROR: {e}")

    # Step 8: Save summary
    summary = {
        "model_name": MODEL_NAME,
        "model_run_name": MODEL_RUN_NAME,
        "model_run_id": model_run.uid,
        "total_files": len(json_files),
        "labels_built": len(labels),
        "labels_ok": total_ok,
        "labels_errors": total_err,
        "no_boxes": no_boxes,
        "total_boxes": total_boxes,
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "test_mode": args.test,
    }
    summary_path = output_dir / "import_b_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\n{'=' * 55}")
    print("SUMMARY — PROJECT B MULTI-SPECIES MODEL RUN")
    print(f"{'=' * 55}")
    print(f"  Model run:     {MODEL_RUN_NAME}")
    print(f"  Model run ID:  {model_run.uid}")
    print(f"  Uploaded OK:   {total_ok} / {len(json_files)}")
    print(f"  Total boxes:   {total_boxes}")
    print(f"  Upload errors: {total_err}")
    print(f"  Summary:       {summary_path}")
    print(f"{'=' * 55}")
    if args.test:
        print("Review in Labelbox, then run without --test.")


if __name__ == "__main__":
    main()
