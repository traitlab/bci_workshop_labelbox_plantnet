"""
Phase 1-multi step b — Import multi-species predictions into Project A Model Run.

Reads 7,717 multi-species JSON files from the Pl@ntNet survey endpoint,
resolves GBIF IDs to WCVP canonical names via the crosswalk, and imports
into Project A as a new Model Run with:

  - [Global] Radio "Taxon"      = highest-coverage species (with nested Cover (%))
  - [Global] Checklist "Taxa"   = all resolved species (each with nested Cover (%))
  - [Tool] BBOX "Plant box"     = one box per species (best tile, score >= threshold)
    - Nested Radio "Taxon"      = species name
    - Nested Checklist "Organs"  = organ from best tile

Model and run names are read from config.yaml:
  plantnet.multi_model_name
  plantnet.multi_model_run_name

Usage:
  python scripts/14_multi_predictions/14b_import_multi_predictions_a.py --test
  python scripts/14_multi_predictions/14b_import_multi_predictions_a.py
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
from labelbox.schema.model_run import DataSplit
from dotenv import load_dotenv
import yaml

PROJECT_A_NAME = "BCI Workshop - All Label Types"
PROJECT_A_UID = "cmn6iicta01w3070sggxmf00q"

BATCH_SIZE = 100
CONFIDENCE_THRESHOLD = 0.05

# Ontology field names
CLS_TAXON   = "Taxon"
CLS_TAXA    = "Taxa"
CLS_ORGANS  = "Organs"
CLS_COVER   = "Cover (%)"
TOOL_BBOX   = "Plant box"

ORGAN_MAP = {
    "leaf":   "Leaf",
    "leaves": "Leaf",
    "flower": "Flower",
    "bloom":  "Flower",
    "fruit":  "Fruit",
    "berry":  "Fruit",
    "branch": "Branch",
    "stem":   "Branch",
    "bark":   "Bark",
}

# Image dimensions for coordinate clamping
IMG_W, IMG_H = 4000, 3000


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk(crosswalk_path: Path, species_path: Path) -> tuple:
    """
    Returns:
      gbif_to_wcvp  : {gbif_id_str -> wcvp_gbif_id}
      wcvp_to_name  : {wcvp_gbif_id -> wcvp_canonical_name}
      name_to_wcvp  : {original_name -> wcvp_gbif_id}
      valid_wcvp_ids: set of wcvp_gbif_ids in Project A species list
    """
    gbif_to_wcvp = {}
    wcvp_to_name = {}
    name_to_wcvp = {}

    with open(crosswalk_path) as f:
        for row in csv.DictReader(f):
            gbif_id = row.get("gbif_backbone_id", "")
            wcvp_id = row.get("wcvp_gbif_id", "")
            canon = row.get("wcvp_canonical_name", "")
            name = row.get("original_name", "")
            if wcvp_id:
                wcvp_to_name[wcvp_id] = canon
                if gbif_id:
                    gbif_to_wcvp[gbif_id] = wcvp_id
                if name:
                    name_to_wcvp[name] = wcvp_id

    valid_wcvp_ids = set()
    with open(species_path) as f:
        for row in csv.DictReader(f):
            wcvp_id = row.get("wcvp_gbif_id", "")
            canon = row.get("wcvp_canonical_name", "")
            name = row.get("original_name", "")
            if wcvp_id:
                valid_wcvp_ids.add(wcvp_id)
                wcvp_to_name[wcvp_id] = canon
                if name:
                    name_to_wcvp[name] = wcvp_id

    return gbif_to_wcvp, wcvp_to_name, name_to_wcvp, valid_wcvp_ids


def resolve_species(gbif_id: str, binomial: str,
                    gbif_to_wcvp: dict, wcvp_to_name: dict,
                    name_to_wcvp: dict, valid_wcvp_ids: set) -> tuple:
    """
    Resolve a species -> (wcvp_canonical_name, wcvp_id, method) or (None, None, reason).
    Only returns species present in Project A's ontology.
    """
    gbif_id = str(gbif_id or "")

    # 1. GBIF ID match
    if gbif_id and gbif_id in gbif_to_wcvp:
        wcvp_id = gbif_to_wcvp[gbif_id]
        if wcvp_id in valid_wcvp_ids:
            canon = wcvp_to_name.get(wcvp_id)
            if canon:
                return canon, wcvp_id, "gbif"

    # 2. Name match
    if binomial and binomial in name_to_wcvp:
        wcvp_id = name_to_wcvp[binomial]
        if wcvp_id in valid_wcvp_ids:
            canon = wcvp_to_name.get(wcvp_id)
            if canon:
                return canon, wcvp_id, "name"

    return None, None, f"unresolved (gbif={gbif_id}, name={binomial})"


def parse_multi_json(json_path: Path) -> dict:
    """Parse a single multi-species JSON file. Returns structured data."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results", {})
    image = results.get("image", {})

    species_list = []
    for sp in results.get("species", []):
        tiles = sp.get("location", [])
        species_list.append({
            "gbif_id": str(sp.get("gbif_id", "")),
            "binomial": sp.get("binomial", ""),
            "name": sp.get("name", ""),
            "coverage": sp.get("coverage", 0.0),
            "max_score": sp.get("max_score", 0.0),
            "count": sp.get("count", 0),
            "tiles": tiles,
        })

    return {
        "width": image.get("width", IMG_W),
        "height": image.get("height", IMG_H),
        "species": species_list,
    }


def build_label(global_key: str, parsed: dict,
                gbif_to_wcvp: dict, wcvp_to_name: dict,
                name_to_wcvp: dict, valid_wcvp_ids: set) -> tuple:
    """
    Build a lb_types.Label for one image from multi-species data.
    Returns (label_or_None, stats_dict).
    """
    combined_gk = "comb_" + global_key
    img_w = parsed["width"]
    img_h = parsed["height"]

    # Resolve all species
    resolved = []
    unresolved_count = 0
    for sp in parsed["species"]:
        canon, wcvp_id, method = resolve_species(
            sp["gbif_id"], sp["binomial"],
            gbif_to_wcvp, wcvp_to_name, name_to_wcvp, valid_wcvp_ids
        )
        if canon is None:
            unresolved_count += 1
            continue
        resolved.append({
            "canon_name": canon,
            "wcvp_id": wcvp_id,
            "coverage": sp["coverage"],
            "max_score": sp["max_score"],
            "tiles": sp["tiles"],
            "method": method,
        })

    if not resolved:
        return None, {"status": "no_resolved_species", "unresolved": unresolved_count}

    # Sort by coverage descending
    resolved.sort(key=lambda r: r["coverage"], reverse=True)

    annotations = []

    # Global Radio "Taxon" — highest-coverage species
    # No confidence on answers: Labelbox requires it consistently across all leaf nodes,
    # which is impossible when mixing typed annotations (Text has no confidence field).
    top = resolved[0]
    annotations.append(
        lb_types.ClassificationAnnotation(
            name=CLS_TAXON,
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(
                    name=top["canon_name"],
                    classifications=[
                        lb_types.ClassificationAnnotation(
                            name=CLS_COVER,
                            value=lb_types.Text(answer=f"{top['coverage'] * 100:.1f}"),
                        )
                    ],
                )
            ),
        )
    )

    # Global Checklist "Taxa" — all resolved species with coverage
    annotations.append(
        lb_types.ClassificationAnnotation(
            name=CLS_TAXA,
            value=lb_types.Checklist(answer=[
                lb_types.ClassificationAnswer(
                    name=sp["canon_name"],
                    classifications=[
                        lb_types.ClassificationAnnotation(
                            name=CLS_COVER,
                            value=lb_types.Text(answer=f"{sp['coverage'] * 100:.1f}"),
                        )
                    ],
                )
                for sp in resolved
            ]),
        )
    )

    # (c) BBOX "Plant box" — one box per resolved species (best tile >= threshold)
    for sp in resolved:
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

        species_cls = lb_types.ClassificationAnnotation(
            name=CLS_TAXON,
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(name=sp["canon_name"])
            ),
        )

        box_classifications = [species_cls]
        lb_organ = ORGAN_MAP.get(best_tile.get("organ", "").lower(), "")
        if lb_organ:
            box_classifications.append(
                lb_types.ClassificationAnnotation(
                    name=CLS_ORGANS,
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

    label = lb_types.Label(
        data={"global_key": combined_gk},
        annotations=annotations,
    )

    n_boxes = sum(1 for a in annotations if isinstance(a, lb_types.ObjectAnnotation))
    return label, {
        "status": "ok",
        "n_resolved": len(resolved),
        "n_unresolved": unresolved_count,
        "n_boxes": n_boxes,
        "top_species": top["canon_name"],
        "top_coverage": top["coverage"],
    }


def load_splits(splits_csv: Path) -> dict:
    """Load split CSV -> {combined_global_key: DataSplit}."""
    SPLIT_MAP = {"train": DataSplit.TRAINING, "valid": DataSplit.VALIDATION, "test": DataSplit.TEST}
    result = {}
    with open(splits_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            gk = "comb_" + row["global_key"]
            split = row.get("split", "").strip().lower()
            if gk not in result and split in SPLIT_MAP:
                result[gk] = SPLIT_MAP[split]
    return result


def get_or_create_model_run(client: lb.Client, project: lb.Project,
                            model_name: str, run_name: str) -> lb.ModelRun:
    """Find or create Model and ModelRun by name."""
    model = next((m for m in client.get_models() if m.name == model_name), None)
    if model is None:
        print(f"  Creating model '{model_name}'...")
        model = client.create_model(
            name=model_name,
            ontology_id=project.ontology().uid,
        )
    else:
        print(f"  Found model '{model_name}' ({model.uid})")

    run = next((r for r in model.model_runs() if r.name == run_name), None)
    if run is None:
        print(f"  Creating model run '{run_name}'...")
        run = model.create_model_run(run_name)
    else:
        print(f"  Found model run '{run_name}' ({run.uid})")

    return run


def main():
    parser = argparse.ArgumentParser(
        description="Import multi-species predictions into Project A Model Run."
    )
    parser.add_argument("--test", action="store_true",
                        help="Process first 5 files only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    crosswalk_path = Path(config["folders"]["crosswalk"]) / "gbif_crosswalk.csv"
    species_path = Path(config["folders"]["species_list"]) / "bci_species_list.csv"
    output_dir = Path(config["folders"]["multi_predictions"])
    output_dir.mkdir(parents=True, exist_ok=True)

    MODEL_NAME = config["plantnet"]["multi_model_name"]
    MODEL_RUN_NAME = config["plantnet"]["multi_model_run_name"]
    multi_dir = Path(config["plantnet"]["multi_predictions_dir"])

    # Step 1: Load crosswalk
    print("Step 1 — Loading crosswalk and species list ...")
    gbif_to_wcvp, wcvp_to_name, name_to_wcvp, valid_wcvp_ids = load_crosswalk(
        crosswalk_path, species_path
    )
    print(f"  {len(gbif_to_wcvp)} GBIF->WCVP mappings, {len(valid_wcvp_ids)} valid Project A taxa")

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
    project = next((p for p in client.get_projects() if p.name == PROJECT_A_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_A_NAME}' not found.")
    if project.uid != PROJECT_A_UID:
        sys.exit(f"ERROR: Project UID mismatch! Expected {PROJECT_A_UID}, got {project.uid}")
    print(f"  Project A confirmed: '{project.name}' ({project.uid})")

    # Step 4: Set up Model Run
    print("\nStep 4 — Setting up Model Run ...")
    model_run = get_or_create_model_run(client, project, MODEL_NAME, MODEL_RUN_NAME)
    print(f"  Model run ready: {model_run.uid}")
    model_run.update_config({"iou_threshold": 0.0})
    print("  IOU threshold set to 0")

    # Step 5: Build labels
    print("\nStep 5 — Building prediction labels ...")
    labels = []
    resolved_gks = []
    stats_totals = {
        "ok": 0, "no_resolved": 0, "total_boxes": 0,
        "total_resolved_species": 0, "total_unresolved_species": 0,
    }

    for jf in json_files:
        # Extract global_key from filename: DJI_...zoom.JPG.json -> DJI_...zoom.JPG
        global_key = jf.name.replace(".json", "")
        try:
            parsed = parse_multi_json(jf)
        except Exception as e:
            print(f"  WARNING: Skipping {jf.name} — parse error: {e}")
            stats_totals["no_resolved"] += 1
            continue
        label, stats = build_label(
            global_key, parsed,
            gbif_to_wcvp, wcvp_to_name, name_to_wcvp, valid_wcvp_ids
        )

        if label is None:
            stats_totals["no_resolved"] += 1
        else:
            labels.append(label)
            resolved_gks.append("comb_" + global_key)
            stats_totals["ok"] += 1
            stats_totals["total_boxes"] += stats["n_boxes"]
            stats_totals["total_resolved_species"] += stats["n_resolved"]
            stats_totals["total_unresolved_species"] += stats["n_unresolved"]

    print(f"  Labels built:      {len(labels)}")
    print(f"  No resolved taxa:  {stats_totals['no_resolved']}")
    print(f"  Total boxes:       {stats_totals['total_boxes']}")
    print(f"  Avg species/image: {stats_totals['total_resolved_species'] / max(len(labels), 1):.1f}")

    if not labels:
        sys.exit("No labels to import.")

    # Step 6: Register data rows
    print(f"\nStep 6 — Registering {len(resolved_gks)} data rows with model run ...")
    model_run.upsert_data_rows(global_keys=resolved_gks)
    print(f"  Registered {len(resolved_gks)} data rows.")

    # Step 6b: Link ground truth
    print(f"\nStep 6b — Linking ground truth labels from Project A ...")
    model_run.upsert_labels(project_id=project.uid)
    print(f"  Ground truth labels linked.")

    # Step 6c: Assign splits
    splits_csv = Path("input/boxes/bci_images_for_plantnet_w_split.csv")
    if splits_csv.exists():
        print(f"\nStep 6c — Assigning splits ...")
        gk_to_split = load_splits(splits_csv)
        split_groups: dict = {}
        for gk in resolved_gks:
            ds = gk_to_split.get(gk)
            if ds:
                split_groups.setdefault(ds, []).append(gk)
        for ds, gks in split_groups.items():
            model_run.assign_data_rows_to_split(global_keys=gks, split=ds)
            print(f"  {ds.value}: {len(gks)}")

    # Step 7: Upload predictions in batches
    print(f"\nStep 7 — Uploading {len(labels)} predictions in batches of {BATCH_SIZE} ...")
    run_ts = int(time.time())
    total_ok = total_err = 0

    for i in range(0, len(labels), BATCH_SIZE):
        batch = labels[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        job = model_run.add_predictions(
            name=f"multi_pred_{run_ts}_b{batch_num}",
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
    total_predictions = len(json_files)
    coverage_pct = 100 * total_ok / total_predictions if total_predictions else 0
    summary = {
        "model_name": MODEL_NAME,
        "model_run_name": MODEL_RUN_NAME,
        "model_run_id": model_run.uid,
        "total_files": total_predictions,
        "labels_built": len(labels),
        "labels_ok": total_ok,
        "labels_errors": total_err,
        "no_resolved_species": stats_totals["no_resolved"],
        "total_boxes": stats_totals["total_boxes"],
        "coverage_pct": round(coverage_pct, 1),
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "test_mode": args.test,
    }
    summary_path = output_dir / "import_a_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\n{'=' * 55}")
    print("SUMMARY — PROJECT A MULTI-SPECIES MODEL RUN")
    print(f"{'=' * 55}")
    print(f"  Model run:       {MODEL_RUN_NAME}")
    print(f"  Model run ID:    {model_run.uid}")
    print(f"  Uploaded OK:     {total_ok} / {total_predictions} ({coverage_pct:.1f}%)")
    print(f"  Total boxes:     {stats_totals['total_boxes']}")
    print(f"  Upload errors:   {total_err}")
    print(f"  Summary:         {summary_path}")
    print(f"{'=' * 55}")
    if args.test:
        print("Review in Labelbox, then run without --test.")


if __name__ == "__main__":
    main()
