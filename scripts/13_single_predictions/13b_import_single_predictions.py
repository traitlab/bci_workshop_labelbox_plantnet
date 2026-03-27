"""
Phase 1b+1c-single — Apply crosswalk and import single-species predictions
into Project A as a Labelbox Model Run.

Reads predictions.json (from 13a), resolves Pl@ntNet GBIF IDs to WCVP
canonical names via the crosswalk, then imports into Project A as a Model Run
with:
  - [Global] Radio "Taxon"         = top-1 species (WCVP canonical name, with confidence)
  - [Global] Checklist "Organs"    = predicted organs (mapped from Pl@ntNet organ strings)

The Model Run is named "PlantNet Single-Species (k-central-america)" and is
created under a Model named "PlantNet Single-Species".

Organ mapping (Pl@ntNet → Labelbox ontology):
  leaf, leaves  → Leaf
  flower, bloom → Flower
  fruit, berry  → Fruit
  branch, stem  → Branch
  bark          → Bark
  (unknown/other organs are skipped)

Usage:
  python scripts/13_single_predictions/13b_import_single_predictions.py --test
  python scripts/13_single_predictions/13b_import_single_predictions.py
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

# ── Safety guard ──────────────────────────────────────────────────────────────
PROJECT_A_NAME = "BCI Workshop - All Label Types"
PROJECT_A_UID  = "cmn6iicta01w3070sggxmf00q"

BATCH_SIZE     = 100

# Pl@ntNet organ string → Labelbox "Organs" checklist option name
ORGAN_MAP = {
    "leaf":    "Leaf",
    "leaves":  "Leaf",
    "flower":  "Flower",
    "bloom":   "Flower",
    "fruit":   "Fruit",
    "berry":   "Fruit",
    "branch":  "Branch",
    "stem":    "Branch",
    "bark":    "Bark",
}


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk(crosswalk_path: Path, species_path: Path) -> tuple:
    """
    Returns:
      gbif_to_wcvp  : {gbif_id_str -> wcvp_gbif_id}   (from crosswalk, GBIF backbone IDs)
      wcvp_to_name  : {wcvp_gbif_id -> wcvp_canonical_name}
      name_to_wcvp  : {original_name -> wcvp_gbif_id} (for name-based fallback)
    """
    gbif_to_wcvp = {}
    wcvp_to_name = {}
    name_to_wcvp = {}

    with open(crosswalk_path) as f:
        for row in csv.DictReader(f):
            gbif_id = row.get("gbif_backbone_id", "")
            wcvp_id = row.get("wcvp_gbif_id", "")
            canon   = row.get("wcvp_canonical_name", "")
            name    = row.get("original_name", "")
            if wcvp_id:
                wcvp_to_name[wcvp_id] = canon
                if gbif_id:
                    gbif_to_wcvp[gbif_id] = wcvp_id
                if name:
                    name_to_wcvp[name] = wcvp_id

    with open(species_path) as f:
        for row in csv.DictReader(f):
            wcvp_id = row.get("wcvp_gbif_id", "")
            canon   = row.get("wcvp_canonical_name", "")
            name    = row.get("original_name", "")
            if wcvp_id:
                wcvp_to_name[wcvp_id] = canon
                if name:
                    name_to_wcvp[name] = wcvp_id

    return gbif_to_wcvp, wcvp_to_name, name_to_wcvp


def resolve_species(result: dict, gbif_to_wcvp: dict,
                    wcvp_to_name: dict, name_to_wcvp: dict) -> tuple:
    """
    Resolve a single result entry → (wcvp_canonical_name, method) or (None, reason).
    Resolution order:
      1. GBIF ID match via crosswalk
      2. Scientific name match in species list
    """
    gbif_id = str(result.get("gbif_id", "") or "")
    sci_name = result.get("scientific_name", "") or ""

    # 1. GBIF ID → WCVP
    if gbif_id and gbif_id in gbif_to_wcvp:
        wcvp_id = gbif_to_wcvp[gbif_id]
        canon   = wcvp_to_name.get(wcvp_id)
        if canon:
            return canon, "gbif"

    # 2. Scientific name → WCVP
    if sci_name and sci_name in name_to_wcvp:
        wcvp_id = name_to_wcvp[sci_name]
        canon   = wcvp_to_name.get(wcvp_id)
        if canon:
            return canon, "name"

    return None, f"unresolved (gbif={gbif_id}, name={sci_name})"


def build_label(entry: dict, gbif_to_wcvp: dict,
                wcvp_to_name: dict, name_to_wcvp: dict) -> tuple:
    """
    Build a lb_types.Label for one image prediction entry.
    Returns (label_or_None, stats_dict).
    """
    combined_gk = "comb_" + entry["global_key"]
    results     = entry.get("results", [])
    organs      = entry.get("organs", [])

    if not results:
        return None, {"status": "no_results"}

    # Resolve top-1 species
    top1 = results[0]
    canon_name, method = resolve_species(top1, gbif_to_wcvp, wcvp_to_name, name_to_wcvp)
    if canon_name is None:
        return None, {"status": f"unresolved_top1: {method}"}

    score = top1.get("score") or 0.0

    annotations = [
        lb_types.ClassificationAnnotation(
            name="Taxon",
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(
                    name=canon_name,
                    confidence=float(score),
                )
            ),
        )
    ]

    # Map organs
    lb_organs = []
    for organ_str in organs:
        lb_organ = ORGAN_MAP.get(organ_str.lower())
        if lb_organ and lb_organ not in lb_organs:
            lb_organs.append(lb_organ)

    if lb_organs:
        annotations.append(
            lb_types.ClassificationAnnotation(
                name="Organs",
                value=lb_types.Checklist(
                    answer=[
                        lb_types.ClassificationAnswer(name=o)
                        for o in lb_organs
                    ]
                ),
            )
        )

    label = lb_types.Label(
        data={"global_key": combined_gk},
        annotations=annotations,
    )
    return label, {
        "status": "ok",
        "method": method,
        "canon_name": canon_name,
        "score": score,
        "organs": lb_organs,
    }


def load_splits(splits_csv: Path) -> dict:
    """Load split CSV -> {combined_global_key: DataSplit}. Deduplicates on first occurrence."""
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
        description="Import single-species predictions into Project A Model Run."
    )
    parser.add_argument("--test", action="store_true",
                        help="Process first 5 predictions only")
    parser.add_argument("--link-gt-only", action="store_true",
                        help="Only link GT labels to existing model run (skip prediction upload)")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    crosswalk_path = Path(config["folders"]["crosswalk"])          / "gbif_crosswalk.csv"
    species_path   = Path(config["folders"]["species_list"])       / "bci_species_list.csv"
    pred_path      = Path(config["folders"]["single_predictions"]) / "predictions.json"
    output_dir     = Path(config["folders"]["single_predictions"])
    output_dir.mkdir(parents=True, exist_ok=True)

    MODEL_NAME     = config["plantnet"]["single_model_name"]
    MODEL_RUN_NAME = config["plantnet"]["single_model_run_name"]

    if not pred_path.exists():
        sys.exit(f"ERROR: {pred_path} not found. Run 13a first.")

    # Step 1: Load crosswalk
    print("Step 1 - Loading crosswalk and species list...")
    gbif_to_wcvp, wcvp_to_name, name_to_wcvp = load_crosswalk(crosswalk_path, species_path)
    print(f"  {len(gbif_to_wcvp)} GBIF->WCVP mappings, {len(wcvp_to_name)} WCVP names")

    # Step 2: Load predictions
    print("\nStep 2 - Loading predictions...")
    entries = json.loads(pred_path.read_text(encoding="utf-8"))
    print(f"  {len(entries)} prediction entries")

    if args.test:
        entries = entries[:5]
        print("  TEST MODE: 5 entries only")

    # Step 3: Connect to Labelbox, verify Project A
    print("\nStep 3 - Connecting to Labelbox...")
    client  = lb.Client(api_key=api_key, enable_experimental=True)
    project = next((p for p in client.get_projects() if p.name == PROJECT_A_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_A_NAME}' not found.")
    if project.uid != PROJECT_A_UID:
        sys.exit(
            f"ERROR: Project UID mismatch!\n"
            f"  Expected: {PROJECT_A_UID}\n"
            f"  Got:      {project.uid}"
        )
    print(f"  Project A confirmed: '{project.name}' ({project.uid})")

    # Step 4: Get or create Model Run
    # If --link-gt-only, skip steps 1-5 and jump straight to GT linking
    print("\nStep 4 - Setting up Model Run...")
    model_run = get_or_create_model_run(client, project, MODEL_NAME, MODEL_RUN_NAME)
    print(f"  Model run ready: {model_run.uid}")

    # Step 5: Build labels
    print("\nStep 5 - Building prediction labels...")
    labels        = []
    resolved_gks  = []
    unresolved    = 0
    no_results    = 0
    method_counts = {}

    for entry in entries:
        label, stats = build_label(entry, gbif_to_wcvp, wcvp_to_name, name_to_wcvp)
        if label is None:
            if stats["status"] == "no_results":
                no_results += 1
            else:
                unresolved += 1
        else:
            labels.append(label)
            resolved_gks.append("comb_" + entry["global_key"])
            m = stats["method"]
            method_counts[m] = method_counts.get(m, 0) + 1

    print(f"  Labels built:   {len(labels)}")
    print(f"  Unresolved:     {unresolved}")
    print(f"  No results:     {no_results}")
    for m, n in sorted(method_counts.items()):
        print(f"  Method '{m}':   {n}")

    if not labels:
        sys.exit("No labels to import.")

    # Step 6: Register data rows with model run
    print(f"\nStep 6 - Registering {len(resolved_gks)} data rows with model run...")
    model_run.upsert_data_rows(global_keys=resolved_gks)
    print(f"  Registered {len(resolved_gks)} data rows.")

    # Step 6b: Link ground truth labels from Project A to the model run
    # Without this, the model run has no GT and metrics cannot be computed.
    print(f"\nStep 6b - Linking ground truth labels from Project A...")
    model_run.upsert_labels(project_id=project.uid)
    print(f"  Ground truth labels linked.")

    # Step 6c: Assign splits to model run data rows
    splits_csv = Path("input/boxes/bci_images_for_plantnet_w_split.csv")
    if splits_csv.exists():
        print(f"\nStep 6c - Assigning splits to model run...")
        gk_to_split = load_splits(splits_csv)
        all_pred_gks = ["comb_" + e["global_key"] for e in entries]
        split_groups: dict = {}
        for gk in all_pred_gks:
            ds = gk_to_split.get(gk)
            if ds:
                split_groups.setdefault(ds, []).append(gk)
        for ds, gks in split_groups.items():
            model_run.assign_data_rows_to_split(global_keys=gks, split=ds)
            print(f"  {ds.value}: {len(gks)}")

    if args.link_gt_only:
        print("\nDone (--link-gt-only: skipped prediction upload).")
        return

    # Step 7: Upload predictions in batches
    print(f"\nStep 7 - Uploading {len(labels)} predictions in batches of {BATCH_SIZE}...")
    run_ts   = int(time.time())
    total_ok = total_err = 0

    for i in range(0, len(labels), BATCH_SIZE):
        batch     = labels[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        job = model_run.add_predictions(
            name=f"single_pred_{run_ts}_b{batch_num}",
            predictions=batch,
        )
        job.wait_till_done()
        errs = job.errors
        ok   = len(batch) - len(errs)
        total_ok  += ok
        total_err += len(errs)
        print(f"  Batch {batch_num}: {ok} ok, {len(errs)} errors "
              f"({total_ok}/{len(labels)} total)")
        if errs:
            for e in errs[:3]:
                print(f"    ERROR: {e}")

    # Step 8: Save summary
    total_predictions = len(entries)
    coverage_pct = 100 * total_ok / total_predictions if total_predictions else 0
    summary = {
        "model_name":         MODEL_NAME,
        "model_run_name":     MODEL_RUN_NAME,
        "model_run_id":       model_run.uid,
        "total_predictions":  total_predictions,
        "labels_built":       len(labels),
        "labels_ok":          total_ok,
        "labels_errors":      total_err,
        "unresolved":         unresolved,
        "no_results":         no_results,
        "coverage_pct":       round(coverage_pct, 1),
        "coverage_note":      (
            f"Labelbox metrics are computed only on the {total_ok} images with a "
            f"resolvable prediction ({coverage_pct:.1f}% of {total_predictions} total). "
            f"{unresolved + no_results} images have no prediction (species outside ontology "
            f"or no API result) and are excluded from metrics — performance may be inflated."
        ),
        "resolution_methods": method_counts,
        "test_mode":          args.test,
    }
    summary_path = output_dir / "import_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\n{'=' * 55}")
    print("SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Model run:       {MODEL_RUN_NAME}")
    print(f"  Model run ID:    {model_run.uid}")
    print(f"  Uploaded OK:     {total_ok} / {total_predictions} "
          f"({coverage_pct:.1f}% coverage)")
    print(f"  Unresolved:      {unresolved + no_results} "
          f"(excluded from metrics — see import_summary.json)")
    print(f"  Upload errors:   {total_err}")
    print(f"  Summary:         {summary_path}")
    print(f"{'=' * 55}")
    if args.test:
        print("Review in Labelbox, then run without --test.")


if __name__ == "__main__":
    main()
