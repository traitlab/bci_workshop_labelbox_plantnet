"""
Phase 0j — Import BBOX ground truth labels into Project A.

Reads crop_bounding_boxes.csv (one row per bounding box, multiple boxes per
image), resolves each lb_label to a WCVP ID via the crosswalk + species list,
and imports ObjectAnnotation (Plant box + nested Radio Taxon) labels into
Project A ('BCI Workshop - All Label Types').

Label resolution strategy (in order):
  1. Direct match on lb_label == original_name in crosswalk/species list
  2. Code match on (6-letter, 4-letter) suffix codes shared between lb_label
     and crosswalk/species original_name
  3. Manual overrides for the 2 cases not resolvable by code

Images are linked via image_url (unique per data row in the combined dataset).

Input:
  input/boxes/crop_bounding_boxes.csv

Output:
  output/11_bbox_gt/bbox_gt_summary.json

Usage:
  python scripts/11_bbox_gt/11_import_bbox_gt.py --test    # 1 image only
  python scripts/11_bbox_gt/11_import_bbox_gt.py           # all images
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import labelbox as lb
import labelbox.types as lb_types
from dotenv import load_dotenv
import yaml

PROJECT_NAME = "BCI Workshop - All Label Types"
COMBINED_KEY_PREFIX = "comb_"
EXPORT_TIMEOUT_SEC = 300
UPLOAD_BATCH_SIZE = 100

# Manual overrides: lb_label -> original_name in crosswalk/species list
MANUAL_OVERRIDES = {
    "Guarea guidonia-GUARGU-GUA2": "Guarea grandifolia-GUARGR-GUA1",
    "Swartzia simplex var. continentalis-SWARS2-SWA2": "Swartzia simplex var. grandiflora-SWARS1-SWA1",
    "Fridericia": "Arrabidaea",
}


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk(crosswalk_path: Path, species_path: Path) -> tuple:
    """
    Returns:
      - label_to_wcvp: {original_name -> wcvp_gbif_id}
      - wcvp_to_name:  {wcvp_gbif_id -> wcvp_canonical_name}
    """
    label_to_wcvp = {}
    wcvp_to_name = {}

    for path in (crosswalk_path, species_path):
        with open(path) as f:
            for row in csv.DictReader(f):
                name = row["original_name"]
                wcvp_id = row["wcvp_gbif_id"]
                canon = row["wcvp_canonical_name"]
                if wcvp_id:
                    label_to_wcvp[name] = wcvp_id
                    wcvp_to_name[wcvp_id] = canon

    return label_to_wcvp, wcvp_to_name


def build_code_index(label_to_wcvp: dict) -> dict:
    """Build {(code6, code4): original_name} from all known labels."""
    index = {}
    for name in label_to_wcvp:
        parts = name.split("-")
        if len(parts) >= 3:
            index[(parts[-2], parts[-1])] = name
    return index


def resolve_label(lb_label: str, label_to_wcvp: dict, code_index: dict) -> tuple:
    """
    Resolve lb_label -> (wcvp_gbif_id, method) or (None, reason).
    """
    # 1. Direct match
    if lb_label in label_to_wcvp:
        return label_to_wcvp[lb_label], "direct"

    # 2. Manual override
    if lb_label in MANUAL_OVERRIDES:
        mapped = MANUAL_OVERRIDES[lb_label]
        if mapped in label_to_wcvp:
            return label_to_wcvp[mapped], "manual"

    # 3. Code match
    parts = lb_label.split("-")
    if len(parts) >= 3:
        key = (parts[-2], parts[-1])
        if key in code_index:
            mapped = code_index[key]
            return label_to_wcvp[mapped], "code"

    return None, "unmatched"


def load_boxes(csv_path: Path) -> dict:
    """
    Load BBOX CSV and return {image_url: [{lb_label, x_min, y_min, x_max, y_max}, ...]}.
    Coordinates are integers.
    """
    url_to_boxes = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url_to_boxes[row["image_url"]].append({
                "lb_label": row["lb_label"],
                "x_min": int(row["x_min"]),
                "y_min": int(row["y_min"]),
                "x_max": int(row["x_max"]),
                "y_max": int(row["y_max"]),
            })
    return dict(url_to_boxes)


def export_combined_dataset(client: lb.Client, dataset_name: str) -> dict:
    """Export combined dataset -> {image_url: combined_global_key}."""
    print(f"Finding dataset '{dataset_name}'...")
    dataset = next((d for d in client.get_datasets() if d.name == dataset_name), None)
    if dataset is None:
        sys.exit(f"ERROR: Dataset '{dataset_name}' not found.")
    print(f"  Found: {dataset.uid}")

    print("Exporting data row IDs...")
    export_task = dataset.export(params={
        "attachments": False,
        "metadata_fields": False,
        "data_row_details": True,
        "embeddings": False,
        "labels": False,
    })
    export_task.wait_till_done(timeout_seconds=EXPORT_TIMEOUT_SEC)

    try:
        errors = []
        export_task.get_buffered_stream(stream_type=lb.StreamType.ERRORS).start(
            stream_handler=lambda output: errors.append(output.json)
        )
        if errors:
            sys.exit(f"ERROR: Export failed: {errors}")
    except ValueError:
        pass

    url_to_gk = {}
    export_task.get_buffered_stream(stream_type=lb.StreamType.RESULT).start(
        stream_handler=lambda output: url_to_gk.update({
            output.json["data_row"]["row_data"]: output.json["data_row"]["global_key"]
        }) if "data_row" in output.json else None
    )
    print(f"  Exported {len(url_to_gk)} data rows")
    return url_to_gk


def build_label(global_key: str, boxes: list, label_to_wcvp: dict,
                code_index: dict, wcvp_to_name: dict) -> tuple:
    """
    Build a lb_types.Label for one image.
    Returns (label, stats_dict).
    """
    annotations = []
    stats = {"boxes_total": len(boxes), "boxes_imported": 0,
             "boxes_skipped": 0, "skip_reasons": []}

    for box in boxes:
        wcvp_id, method = resolve_label(box["lb_label"], label_to_wcvp, code_index)
        if wcvp_id is None:
            stats["boxes_skipped"] += 1
            stats["skip_reasons"].append(box["lb_label"])
            continue

        canon_name = wcvp_to_name.get(wcvp_id)
        if canon_name is None:
            stats["boxes_skipped"] += 1
            stats["skip_reasons"].append(f"{box['lb_label']} (no name for wcvp {wcvp_id})")
            continue

        x_min, y_min = box["x_min"], box["y_min"]
        x_max, y_max = box["x_max"], box["y_max"]
        w = x_max - x_min
        h = y_max - y_min
        if w <= 0 or h <= 0:
            stats["boxes_skipped"] += 1
            stats["skip_reasons"].append(f"{box['lb_label']} (invalid bbox {w}x{h})")
            continue

        annotations.append(
            lb_types.ObjectAnnotation(
                name="Plant box",
                value=lb_types.Rectangle(
                    start=lb_types.Point(x=x_min, y=y_min),
                    end=lb_types.Point(x=x_max, y=y_max),
                ),
                classifications=[
                    lb_types.ClassificationAnnotation(
                        name="Taxon",
                        value=lb_types.Radio(
                            answer=lb_types.ClassificationAnswer(name=canon_name)
                        ),
                    )
                ],
            )
        )
        stats["boxes_imported"] += 1

    if not annotations:
        return None, stats

    label = lb_types.Label(
        data={"global_key": global_key},
        annotations=annotations,
    )
    return label, stats


def main():
    parser = argparse.ArgumentParser(description="Import BBOX GT labels into Project A.")
    parser.add_argument("--test", action="store_true", help="Process 1 image only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    crosswalk_path = Path(config["folders"]["crosswalk"]) / "gbif_crosswalk.csv"
    species_path = Path(config["folders"]["species_list"]) / "bci_species_list.csv"
    bbox_csv = Path("input/boxes/crop_bounding_boxes.csv")
    output_dir = Path("output/11_bbox_gt")
    output_dir.mkdir(parents=True, exist_ok=True)
    combined_name = config["labelbox"]["combined_dataset_name"]

    # Step 1: Load crosswalk + species list
    print("Step 1 - Loading crosswalk and species list...")
    label_to_wcvp, wcvp_to_name = load_crosswalk(crosswalk_path, species_path)
    code_index = build_code_index(label_to_wcvp)
    print(f"  {len(label_to_wcvp)} known labels, {len(code_index)} code-indexed")

    # Step 2: Load BBOX data
    print("\nStep 2 - Loading BBOX CSV...")
    url_to_boxes = load_boxes(bbox_csv)
    print(f"  {sum(len(v) for v in url_to_boxes.values())} boxes across {len(url_to_boxes)} images")

    if args.test:
        first_url = next(iter(url_to_boxes))
        url_to_boxes = {first_url: url_to_boxes[first_url]}
        print(f"  TEST MODE: 1 image ({len(url_to_boxes[first_url])} boxes)")

    # Step 3: Connect to Labelbox, export combined dataset
    print("\nStep 3 - Fetching combined dataset data rows...")
    client = lb.Client(api_key=api_key)
    url_to_gk = export_combined_dataset(client, combined_name)

    # Step 4: Get Project A
    print(f"\nStep 4 - Finding project '{PROJECT_NAME}'...")
    project = next((p for p in client.get_projects() if p.name == PROJECT_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_NAME}' not found.")
    print(f"  Found: {project.uid}")

    # Step 5: Build labels
    print("\nStep 5 - Building labels...")
    labels = []
    total_boxes = skipped_boxes = skipped_images = 0
    unmatched_labels = defaultdict(int)

    for url, boxes in url_to_boxes.items():
        combined_gk = url_to_gk.get(url)
        if combined_gk is None:
            skipped_images += 1
            continue

        label, stats = build_label(combined_gk, boxes, label_to_wcvp, code_index, wcvp_to_name)
        total_boxes += stats["boxes_total"]
        skipped_boxes += stats["boxes_skipped"]
        for r in stats["skip_reasons"]:
            unmatched_labels[r] += 1

        if label is not None:
            labels.append(label)

    print(f"  Labels to import: {len(labels)}")
    print(f"  Boxes to import:  {total_boxes - skipped_boxes}")
    print(f"  Boxes skipped:    {skipped_boxes}")
    if skipped_images:
        print(f"  Images skipped (no data row): {skipped_images}")
    if unmatched_labels:
        print(f"  Unmatched label breakdown:")
        for lbl, n in sorted(unmatched_labels.items(), key=lambda x: -x[1]):
            print(f"    {n:>4}x  {lbl}")

    if not labels:
        sys.exit("No labels to import.")

    # Step 6: Upload
    print(f"\nStep 6 - Uploading {len(labels)} labels in batches of {UPLOAD_BATCH_SIZE}...")
    run_ts = int(time.time())
    total_ok = total_err = 0
    for i in range(0, len(labels), UPLOAD_BATCH_SIZE):
        batch = labels[i:i + UPLOAD_BATCH_SIZE]
        upload_job = lb.LabelImport.create_from_objects(
            client=client,
            project_id=project.uid,
            name=f"bbox_gt_{run_ts}_batch_{i // UPLOAD_BATCH_SIZE + 1}",
            labels=batch,
        )
        upload_job.wait_till_done()
        errors = upload_job.errors
        ok = len(batch) - len(errors)
        total_ok += ok
        total_err += len(errors)
        print(f"  Batch {i // UPLOAD_BATCH_SIZE + 1}: {ok} ok, {len(errors)} errors "
              f"({total_ok}/{len(labels)} total)")
        if errors:
            for e in errors[:3]:
                print(f"    ERROR: {e}")

    # Step 7: Summary
    summary = {
        "images_processed": len(labels) + skipped_images,
        "images_with_labels": len(labels),
        "images_skipped_no_data_row": skipped_images,
        "boxes_total": total_boxes,
        "boxes_imported": total_boxes - skipped_boxes,
        "boxes_skipped": skipped_boxes,
        "labels_uploaded_ok": total_ok,
        "labels_upload_errors": total_err,
        "unmatched_labels": dict(unmatched_labels),
        "test_mode": args.test,
    }
    summary_path = output_dir / "bbox_gt_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 55}")
    print("SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Images with labels uploaded: {total_ok} / {len(labels)}")
    print(f"  Boxes imported:  {total_boxes - skipped_boxes}")
    print(f"  Boxes skipped:   {skipped_boxes}")
    print(f"  Upload errors:   {total_err}")
    print(f"  Summary:         {summary_path}")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
