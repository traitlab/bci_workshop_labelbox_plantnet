"""
Phase 0g+0j (combined) — Import all ground truth labels into Project A.

Replaces the separate Phase 0g (mask GT) and Phase 0j (BBOX GT) scripts.
Deletes ALL existing labels from Project A, then re-imports everything as a
single label per image so that mask annotations and BBOX annotations are
colocated on the same label layer.

Project A is identified by BOTH name AND hardcoded UID as a safety guard.
The delete step requires --confirm-delete on the CLI.

Annotations built per image:
  - [Global] Radio "Taxon"          = WCVP canonical name of dominant-pixel taxon
  - [Global] Checklist "Taxa"       = all taxa present
  - [Tool] Raster Seg "Plant mask"  = one mask per unique mask PNG (nested Checklist "Taxa")
  - [Tool] BBOX "Plant box"         = one box per resolved bbox row (nested Radio "Taxon")

Images with only mask data → mask annotations only (no BBOX).
Images with only BBOX data → BBOX annotations only (no mask). Should not occur
  in practice since all BBOXes are derived from masks.

Three-stage safety protocol:
  Stage 1: 3 mask-labelled images (+ any BBOX matches) — review before proceeding
  Stage 2: one full dataset                             — review before proceeding
  Stage 3: all datasets

Usage:
  python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 1
  python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 2 --dataset 2024_bci_XXXX
  python scripts/12_import_gt_combined/12_import_gt_combined.py --stage 3 --confirm-delete
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import requests
from PIL import Image
import labelbox as lb
import labelbox.types as lb_types
from dotenv import load_dotenv
import yaml

# ── Safety guard ─────────────────────────────────────────────────────────────
# This script ONLY touches Project A. Both name and UID must match.
PROJECT_A_NAME = "BCI Workshop - All Label Types"
PROJECT_A_UID  = "cmn6iicta01w3070sggxmf00q"

COMBINED_KEY_PREFIX = "comb_"
MASK_WORKERS   = 30
BATCH_SIZE     = 100
MASK_RETRY     = 3

# Label resolution: lb_label → original_name in crosswalk/species list
MANUAL_OVERRIDES = {
    "Guarea guidonia-GUARGU-GUA2":                        "Guarea grandifolia-GUARGR-GUA1",
    "Swartzia simplex var. continentalis-SWARS2-SWA2":    "Swartzia simplex var. grandiflora-SWARS1-SWA1",
    "Fridericia":                                          "Arrabidaea",
}


# ── Config / crosswalk helpers ────────────────────────────────────────────────

def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk_and_species(crosswalk_path: Path, species_path: Path) -> tuple:
    """
    Returns:
      label_to_wcvp : {original_name -> wcvp_gbif_id}   (for BBOX resolution)
      wcvp_to_name  : {wcvp_gbif_id  -> wcvp_canonical_name}
      gbif_to_wcvp  : {gbif_backbone_id -> wcvp_gbif_id} (for mask resolution)
    """
    label_to_wcvp = {}
    wcvp_to_name  = {}
    gbif_to_wcvp  = {}

    for path in (crosswalk_path, species_path):
        with open(path) as f:
            for row in csv.DictReader(f):
                name    = row["original_name"]
                wcvp_id = row["wcvp_gbif_id"]
                canon   = row["wcvp_canonical_name"]
                if wcvp_id:
                    label_to_wcvp[name]  = wcvp_id
                    wcvp_to_name[wcvp_id] = canon

    # gbif_backbone_id column only exists in gbif_crosswalk.csv
    with open(crosswalk_path) as f:
        for row in csv.DictReader(f):
            gbif_id = row.get("gbif_backbone_id", "")
            wcvp_id = row.get("wcvp_gbif_id", "")
            if gbif_id and wcvp_id:
                gbif_to_wcvp[gbif_id] = wcvp_id

    return label_to_wcvp, wcvp_to_name, gbif_to_wcvp


def build_code_index(label_to_wcvp: dict) -> dict:
    """Build {(code6, code4): original_name} from all known labels."""
    index = {}
    for name in label_to_wcvp:
        parts = name.split("-")
        if len(parts) >= 3:
            index[(parts[-2], parts[-1])] = name
    return index


def resolve_bbox_label(lb_label: str, label_to_wcvp: dict, code_index: dict) -> tuple:
    """Resolve a bbox lb_label → (wcvp_gbif_id, method) or (None, reason)."""
    if lb_label in label_to_wcvp:
        return label_to_wcvp[lb_label], "direct"
    if lb_label in MANUAL_OVERRIDES:
        mapped = MANUAL_OVERRIDES[lb_label]
        if mapped in label_to_wcvp:
            return label_to_wcvp[mapped], "manual"
    parts = lb_label.split("-")
    if len(parts) >= 3:
        key = (parts[-2], parts[-1])
        if key in code_index:
            return label_to_wcvp[code_index[key]], "code"
    return None, "unmatched"


# ── Mask helpers (Phase 0g logic) ─────────────────────────────────────────────

def url_to_cache_path(cache_dir: Path, url: str) -> Path:
    h = hashlib.md5(url.encode()).hexdigest()
    return cache_dir / f"{h}.png"


def download_mask(url: str, api_key: str, cache_dir: Path) -> bytes | None:
    cache_path = url_to_cache_path(cache_dir, url)
    if cache_path.exists():
        return cache_path.read_bytes()
    headers = {"Authorization": f"Bearer {api_key}"}
    for _ in range(MASK_RETRY):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                cache_path.write_bytes(resp.content)
                return resp.content
        except requests.RequestException:
            pass
        time.sleep(1)
    return None


def count_mask_pixels(mask_bytes: bytes) -> int:
    img = Image.open(io.BytesIO(mask_bytes)).convert("RGBA")
    return int((np.array(img)[:, :, 3] > 0).sum())


def collect_mask_urls(row: dict, gbif_to_wcvp: dict) -> list[dict]:
    entries = []
    for proj in row.get("projects", {}).values():
        for label in proj.get("labels", []):
            for obj in label.get("annotations", {}).get("objects", []):
                classifications = obj.get("classifications", [])
                if not classifications:
                    continue
                answers = classifications[0].get("checklist_answers", [])
                if not answers:
                    continue
                mask_url = obj.get("mask", {}).get("url")
                if not mask_url:
                    continue
                wcvp_ids = [gbif_to_wcvp[a["value"]] for a in answers if gbif_to_wcvp.get(a["value"])]
                if wcvp_ids:
                    entries.append({"url": mask_url, "wcvp_ids": wcvp_ids})
    return entries


def build_mask_annotations(row: dict, gbif_to_wcvp: dict, wcvp_to_name: dict,
                            api_key: str, cache_dir: Path) -> tuple:
    """
    Returns (annotations_list, taxa_pixel_counts) for mask-derived annotations.
    annotations_list contains Radio, Checklist, and Plant mask ObjectAnnotations.
    taxa_pixel_counts is {wcvp_id: pixel_count} for dominant taxon calculation.
    Returns ([], {}) if no valid masks found.
    """
    mask_entries_info = collect_mask_urls(row, gbif_to_wcvp)
    if not mask_entries_info:
        return [], {}

    url_to_wcvp_ids: dict[str, list] = defaultdict(list)
    for info in mask_entries_info:
        for wid in info["wcvp_ids"]:
            if wid not in url_to_wcvp_ids[info["url"]]:
                url_to_wcvp_ids[info["url"]].append(wid)

    mask_entries = []
    taxa_pixel_counts: dict[str, int] = defaultdict(int)
    for url, wcvp_ids in url_to_wcvp_ids.items():
        mask_bytes = download_mask(url, api_key, cache_dir)
        if mask_bytes is None:
            continue
        pixel_count = count_mask_pixels(mask_bytes)
        if pixel_count == 0:
            continue
        px_per_taxon = max(1, pixel_count // len(wcvp_ids))
        for i, wcvp_id in enumerate(wcvp_ids):
            taxa_pixel_counts[wcvp_id] += px_per_taxon
            if i == 0:
                mask_entries.append({
                    "wcvp_id": wcvp_id,
                    "mask_bytes": mask_bytes,
                })

    if not mask_entries:
        return [], {}

    taxa_present      = set(taxa_pixel_counts.keys())
    dominant_wcvp_id  = max(taxa_pixel_counts, key=taxa_pixel_counts.get)
    dominant_name     = wcvp_to_name.get(dominant_wcvp_id, dominant_wcvp_id)

    annotations = [
        lb_types.ClassificationAnnotation(
            name="Taxon",
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(name=dominant_name)
            ),
        ),
        lb_types.ClassificationAnnotation(
            name="Taxa",
            value=lb_types.Checklist(
                answer=[
                    lb_types.ClassificationAnswer(name=wcvp_to_name[wid])
                    for wid in sorted(taxa_present)
                    if wid in wcvp_to_name
                ]
            ),
        ),
    ]

    for entry in mask_entries:
        wcvp_id = entry["wcvp_id"]
        if wcvp_id not in wcvp_to_name:
            continue
        annotations.append(
            lb_types.ObjectAnnotation(
                name="Plant mask",
                value=lb_types.Mask(
                    mask=lb_types.MaskData(im_bytes=entry["mask_bytes"]),
                    color=(255, 255, 255),
                ),
                classifications=[
                    lb_types.ClassificationAnnotation(
                        name="Taxa",
                        value=lb_types.Checklist(
                            answer=[lb_types.ClassificationAnswer(name=wcvp_to_name[wcvp_id])]
                        ),
                    )
                ],
            )
        )

    return annotations, taxa_pixel_counts


# ── BBOX helpers (Phase 0j logic) ─────────────────────────────────────────────

def load_bbox_by_url(csv_path: Path) -> dict:
    """Load BBOX CSV → {image_url: [{lb_label, x_min, y_min, x_max, y_max}, ...]}."""
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


def build_bbox_annotations(boxes: list, label_to_wcvp: dict, code_index: dict,
                            wcvp_to_name: dict) -> tuple:
    """
    Returns (annotations_list, skipped_count, skip_reasons).
    """
    annotations = []
    skipped = 0
    skip_reasons = []

    for box in boxes:
        wcvp_id, _ = resolve_bbox_label(box["lb_label"], label_to_wcvp, code_index)
        if wcvp_id is None:
            skipped += 1
            skip_reasons.append(box["lb_label"])
            continue
        canon_name = wcvp_to_name.get(wcvp_id)
        if canon_name is None:
            skipped += 1
            skip_reasons.append(f"{box['lb_label']} (no name for wcvp {wcvp_id})")
            continue
        # Clamp to BCI image bounds (4000×3000). Source data has occasional coordinates
        # a few pixels outside this range (e.g. x=4002), which Labelbox rejects.
        x_min = max(0, min(box["x_min"], 3999))
        y_min = max(0, min(box["y_min"], 2999))
        x_max = max(0, min(box["x_max"], 3999))
        y_max = max(0, min(box["y_max"], 2999))
        w = x_max - x_min
        h = y_max - y_min
        if w <= 0 or h <= 0:
            skipped += 1
            skip_reasons.append(f"{box['lb_label']} (invalid bbox {w}x{h})")
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

    return annotations, skipped, skip_reasons


# ── Dataset / export helpers ───────────────────────────────────────────────────

def collect_rows(exports_dir: Path, stage: int, dataset_name: str | None) -> list[dict]:
    files = sorted(exports_dir.glob("2024_bci_*.json"))
    if stage == 1:
        files = files[:1]
    elif stage == 2:
        if not dataset_name:
            sys.exit("ERROR: --dataset required for stage 2")
        files = [exports_dir / f"{dataset_name}.json"]
        if not files[0].exists():
            sys.exit(f"ERROR: {files[0]} not found")

    rows = []
    for f in files:
        rows.extend(json.load(open(f)))

    labeled = [
        row for row in rows
        if any(proj.get("labels") for proj in row.get("projects", {}).values())
    ]
    if stage == 1:
        labeled = labeled[:3]
    return labeled


def export_combined_dataset(client: lb.Client, dataset_name: str,
                             timeout: int = 300) -> dict:
    """Export combined dataset → {image_url: combined_global_key}."""
    print(f"Finding dataset '{dataset_name}'...")
    dataset = next((d for d in client.get_datasets() if d.name == dataset_name), None)
    if dataset is None:
        sys.exit(f"ERROR: Dataset '{dataset_name}' not found.")
    print(f"  Found: {dataset.uid}")

    print("Exporting data row IDs...")
    export_task = dataset.export(params={
        "attachments": False, "metadata_fields": False,
        "data_row_details": True, "embeddings": False, "labels": False,
    })
    export_task.wait_till_done(timeout_seconds=timeout)

    try:
        errors = []
        export_task.get_buffered_stream(stream_type=lb.StreamType.ERRORS).start(
            stream_handler=lambda o: errors.append(o.json)
        )
        if errors:
            sys.exit(f"ERROR: Export failed: {errors}")
    except ValueError:
        pass

    url_to_gk = {}
    export_task.get_buffered_stream(stream_type=lb.StreamType.RESULT).start(
        stream_handler=lambda o: url_to_gk.update({
            o.json["data_row"]["row_data"]: o.json["data_row"]["global_key"]
        }) if "data_row" in o.json else None
    )
    print(f"  Exported {len(url_to_gk)} data rows")
    return url_to_gk


# ── Delete helpers ─────────────────────────────────────────────────────────────

def delete_project_labels(project: lb.Project) -> int:
    """Delete all labels from the given project. Returns count deleted."""
    print(f"Fetching labels to delete from project '{project.name}' ({project.uid})...")
    labels = list(project.labels())
    print(f"  Found {len(labels)} labels to delete.")
    if not labels:
        return 0
    for i, label in enumerate(labels, 1):
        label.delete()
        if i % 100 == 0 or i == len(labels):
            print(f"  Deleted {i}/{len(labels)}...")
    return len(labels)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Combined GT import (masks + BBOX) for Project A."
    )
    parser.add_argument("--stage", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--dataset", type=str, help="Dataset name for stage 2")
    parser.add_argument("--confirm-delete", action="store_true",
                        help="Required for stage 3 to confirm label deletion")
    args = parser.parse_args()

    if args.stage == 3 and not args.confirm_delete:
        sys.exit("ERROR: --confirm-delete required for stage 3 (deletes all labels in Project A).")

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    crosswalk_path  = Path(config["folders"]["crosswalk"])  / "gbif_crosswalk.csv"
    species_path    = Path(config["folders"]["species_list"]) / "bci_species_list.csv"
    bbox_csv        = Path("input/boxes/crop_bounding_boxes.csv")
    exports_dir     = Path(config["folders"]["exports"])
    cache_dir       = Path(config["folders"]["output"]) / "07_gt_masks_cache"
    combined_name   = config["labelbox"]["combined_dataset_name"]
    output_dir      = Path("output/12_import_gt_combined")
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Load crosswalk + species list ─────────────────────────────────
    print("Step 1 - Loading crosswalk and species list...")
    label_to_wcvp, wcvp_to_name, gbif_to_wcvp = load_crosswalk_and_species(
        crosswalk_path, species_path
    )
    code_index = build_code_index(label_to_wcvp)
    print(f"  {len(label_to_wcvp)} name-indexed, {len(code_index)} code-indexed, "
          f"{len(gbif_to_wcvp)} GBIF-indexed")

    # ── Step 2: Load BBOX CSV ─────────────────────────────────────────────────
    print("\nStep 2 - Loading BBOX CSV...")
    url_to_boxes = load_bbox_by_url(bbox_csv)
    total_bbox_images = len(url_to_boxes)
    total_bbox_boxes  = sum(len(v) for v in url_to_boxes.values())
    print(f"  {total_bbox_boxes} boxes across {total_bbox_images} images")

    # ── Step 3: Connect to Labelbox ───────────────────────────────────────────
    print("\nStep 3 - Connecting to Labelbox...")
    client = lb.Client(api_key=api_key)

    # Safety guard: verify project by BOTH name and UID
    project = next((p for p in client.get_projects() if p.name == PROJECT_A_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_A_NAME}' not found.")
    if project.uid != PROJECT_A_UID:
        sys.exit(
            f"ERROR: Project UID mismatch!\n"
            f"  Expected: {PROJECT_A_UID}\n"
            f"  Got:      {project.uid}\n"
            f"Aborting to protect data."
        )
    print(f"  Project A confirmed: '{project.name}' ({project.uid})")

    # ── Step 4: Export combined dataset (URL → global_key mapping) ────────────
    print("\nStep 4 - Fetching combined dataset data rows...")
    url_to_gk = export_combined_dataset(client, combined_name)

    # ── Step 5: Collect export rows for this stage ────────────────────────────
    print(f"\nStep 5 - Collecting stage {args.stage} export rows...")
    rows = collect_rows(exports_dir, args.stage, args.dataset)
    print(f"  {len(rows)} labeled images to process")

    # ── Step 6: Delete existing labels (stage 3 only, or if any exist in 1/2) ─
    if args.stage == 3:
        print(f"\nStep 6 - Deleting ALL existing labels from Project A ({project.uid})...")
        deleted = delete_project_labels(project)
        print(f"  Deleted {deleted} labels.")
    else:
        print(f"\nStep 6 - Stage {args.stage}: skipping delete (only stage 3 deletes).")

    # ── Step 7: Build combined labels ─────────────────────────────────────────
    print(f"\nStep 7 - Building combined labels...")

    labels          = []
    skipped_images  = 0
    total_mask_annotations = 0
    total_bbox_boxes_imported = 0
    total_bbox_boxes_skipped  = 0
    unmatched_bbox  = defaultdict(int)

    def process_row(row):
        image_url   = row["data_row"].get("row_data", "")
        orig_gk     = row["data_row"]["global_key"]
        combined_gk = COMBINED_KEY_PREFIX + orig_gk

        # Mask annotations
        mask_anns, _ = build_mask_annotations(
            row, gbif_to_wcvp, wcvp_to_name, api_key, cache_dir
        )

        # BBOX annotations
        boxes = url_to_boxes.get(image_url, [])
        bbox_anns, bbox_skipped, bbox_reasons = build_bbox_annotations(
            boxes, label_to_wcvp, code_index, wcvp_to_name
        )

        all_annotations = mask_anns + bbox_anns
        if not all_annotations:
            return None, 0, 0, 0, []

        label = lb_types.Label(
            data={"global_key": combined_gk},
            annotations=all_annotations,
        )
        return label, len(mask_anns), len(bbox_anns), bbox_skipped, bbox_reasons

    done = 0
    with ThreadPoolExecutor(max_workers=MASK_WORKERS) as executor:
        futures = {executor.submit(process_row, row): row for row in rows}
        for future in as_completed(futures):
            done += 1
            row = futures[future]
            orig_gk = row["data_row"]["global_key"]
            try:
                label, n_mask, n_bbox, n_skipped, reasons = future.result()
                if label:
                    labels.append(label)
                    total_mask_annotations     += n_mask
                    total_bbox_boxes_imported  += n_bbox
                    total_bbox_boxes_skipped   += n_skipped
                    for r in reasons:
                        unmatched_bbox[r] += 1
                    status = f"OK (mask={n_mask}, bbox={n_bbox})"
                else:
                    skipped_images += 1
                    status = "skipped (no annotations)"
            except Exception as e:
                skipped_images += 1
                status = f"ERROR: {e}"
            if done % 100 == 0 or done <= 5:
                print(f"  [{done}/{len(rows)}] {orig_gk} ... {status}")

    print(f"\n  Labels built:          {len(labels)}")
    print(f"  Images skipped:        {skipped_images}")
    print(f"  Mask annotations:      {total_mask_annotations}")
    print(f"  BBOX boxes imported:   {total_bbox_boxes_imported}")
    print(f"  BBOX boxes skipped:    {total_bbox_boxes_skipped}")
    if unmatched_bbox:
        print("  Unmatched BBOX labels:")
        for lbl, n in sorted(unmatched_bbox.items(), key=lambda x: -x[1]):
            print(f"    {n:>4}x  {lbl}")

    if not labels:
        sys.exit("No labels to import.")

    # ── Step 8: Upload in batches ─────────────────────────────────────────────
    print(f"\nStep 8 - Uploading {len(labels)} labels in batches of {BATCH_SIZE}...")
    run_ts   = int(time.time())
    total_ok = total_err = 0
    for i in range(0, len(labels), BATCH_SIZE):
        batch      = labels[i:i + BATCH_SIZE]
        batch_num  = i // BATCH_SIZE + 1
        import_job = lb.LabelImport.create_from_objects(
            client=client,
            project_id=project.uid,
            name=f"gt_combined_{run_ts}_b{batch_num}",
            labels=batch,
        )
        import_job.wait_till_done()
        errors = import_job.errors
        ok     = len(batch) - len(errors)
        total_ok  += ok
        total_err += len(errors)
        print(f"  Batch {batch_num}: {ok} ok, {len(errors)} errors "
              f"({total_ok}/{len(labels)} total)")
        if errors:
            for e in errors[:3]:
                print(f"    ERROR: {e}")

    # ── Step 9: Save summary ──────────────────────────────────────────────────
    summary = {
        "stage": args.stage,
        "images_processed":         len(rows),
        "images_with_labels":       len(labels),
        "images_skipped":           skipped_images,
        "mask_annotations_built":   total_mask_annotations,
        "bbox_boxes_imported":      total_bbox_boxes_imported,
        "bbox_boxes_skipped":       total_bbox_boxes_skipped,
        "labels_uploaded_ok":       total_ok,
        "labels_upload_errors":     total_err,
        "unmatched_bbox_labels":    dict(unmatched_bbox),
    }
    summary_path = output_dir / f"summary_stage{args.stage}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 55}")
    print("SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Labels uploaded:    {total_ok} / {len(labels)}")
    print(f"  Mask annotations:   {total_mask_annotations}")
    print(f"  BBOX boxes:         {total_bbox_boxes_imported}")
    print(f"  Upload errors:      {total_err}")
    print(f"  Summary:            {summary_path}")
    print(f"{'=' * 55}")
    if args.stage < 3:
        print(f"Review in Labelbox, then run stage {args.stage + 1}.")


if __name__ == "__main__":
    main()
