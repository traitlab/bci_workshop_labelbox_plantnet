"""
Phase 0g — Import ground truth labels into Project A.

For each labeled image, builds a Label with:
  - [Global] Radio "Dominant taxon"   = WCVP ID of the taxon with most mask pixels
  - [Global] Checklist "Taxa present" = WCVP IDs of all taxa annotated in the image
  - [Tool] Raster Seg "Plant mask"    = one mask per annotated object (downloaded from Labelbox)
    with nested Radio "Taxon" = WCVP ID

Masks with no taxon assignment are skipped.
For multi-answer masks (botanist selected 2 taxa for one mask), each answer counts
as a separate taxon presence, and pixel count is split equally between them.
Taxa whose GBIF backbone ID has no WCVP match are skipped.

Three-stage safety protocol:
  Stage 1: 3 images from first dataset — review before proceeding
  Stage 2: one full dataset — review before proceeding
  Stage 3: all datasets

Usage:
  python scripts/07_import_gt/07_import_ground_truth.py --stage 1
  python scripts/07_import_gt/07_import_ground_truth.py --stage 2 --dataset 2024_bci_XXXX
  python scripts/07_import_gt/07_import_ground_truth.py --stage 3
"""

import argparse
import csv
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

MASK_WORKERS = 8      # concurrent mask downloads
BATCH_SIZE = 100      # labels per import batch
MASK_RETRY = 3        # retries per mask download


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk(crosswalk_path: Path) -> dict:
    """Returns {gbif_backbone_id: wcvp_gbif_id} for taxa with a WCVP match."""
    xwalk = {}
    for row in csv.DictReader(open(crosswalk_path)):
        if row["wcvp_gbif_id"]:
            xwalk[row["gbif_backbone_id"]] = row["wcvp_gbif_id"]
    return xwalk


def load_species_list(species_list_path: Path) -> dict:
    """Returns {wcvp_gbif_id: wcvp_canonical_name}."""
    return {
        row["wcvp_gbif_id"]: row["wcvp_canonical_name"]
        for row in csv.DictReader(open(species_list_path))
        if row["wcvp_gbif_id"]
    }


def download_mask(url: str, api_key: str) -> bytes | None:
    """Download a mask PNG, with retries. Returns bytes or None on failure."""
    headers = {"Authorization": f"Bearer {api_key}"}
    for attempt in range(MASK_RETRY):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.content
        except requests.RequestException:
            pass
        time.sleep(1)
    return None


def count_mask_pixels(mask_bytes: bytes) -> int:
    """Count non-background pixels in a mask PNG (checks alpha channel if RGBA, else non-zero)."""
    img = Image.open(io.BytesIO(mask_bytes)).convert("RGBA")
    arr = np.array(img)
    return int((arr[:, :, 3] > 0).sum())


def process_image(row: dict, crosswalk: dict, wcvp_names: dict, api_key: str) -> lb_types.Label | None:
    """
    Build a Label for one image.
    Returns None if no labeled masks with valid WCVP IDs.
    """
    global_key = "comb_" + row["data_row"]["global_key"]

    # Collect all annotated objects with taxon info
    mask_entries = []  # list of {wcvp_id, mask_bytes, pixel_count}
    taxa_present = set()

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

                # Get WCVP IDs for all answers
                wcvp_ids = []
                for ans in answers:
                    gbif_id = ans.get("value", "")
                    wcvp_id = crosswalk.get(gbif_id)
                    if wcvp_id:
                        wcvp_ids.append(wcvp_id)

                if not wcvp_ids:
                    continue

                # Download mask
                mask_bytes = download_mask(mask_url, api_key)
                if mask_bytes is None:
                    continue

                pixel_count = count_mask_pixels(mask_bytes)
                if pixel_count == 0:
                    continue

                # Split pixels equally among multi-answer taxa
                px_per_taxon = pixel_count // len(wcvp_ids)
                for wcvp_id in wcvp_ids:
                    taxa_present.add(wcvp_id)
                    mask_entries.append({
                        "wcvp_id": wcvp_id,
                        "mask_bytes": mask_bytes,
                        "pixel_count": px_per_taxon,
                    })

    if not taxa_present:
        return None

    # Find dominant taxon (most total pixels)
    pixel_totals = defaultdict(int)
    for entry in mask_entries:
        pixel_totals[entry["wcvp_id"]] += entry["pixel_count"]
    dominant_wcvp_id = max(pixel_totals, key=pixel_totals.get)
    dominant_name = wcvp_names.get(dominant_wcvp_id, dominant_wcvp_id)

    # Build annotations
    annotations = []

    # Radio: Dominant taxon
    annotations.append(
        lb_types.ClassificationAnnotation(
            name="Dominant taxon",
            value=lb_types.Radio(
                answer=lb_types.ClassificationAnswer(name=dominant_name)
            ),
        )
    )

    # Checklist: Taxa present
    annotations.append(
        lb_types.ClassificationAnnotation(
            name="Taxa present",
            value=lb_types.Checklist(
                answer=[
                    lb_types.ClassificationAnswer(name=wcvp_names[wid])
                    for wid in sorted(taxa_present)
                    if wid in wcvp_names
                ]
            ),
        )
    )

    # Raster Seg masks
    for entry in mask_entries:
        wcvp_id = entry["wcvp_id"]
        if wcvp_id not in wcvp_names:
            continue
        taxon_name = wcvp_names[wcvp_id]
        annotations.append(
            lb_types.ObjectAnnotation(
                name="Plant mask",
                value=lb_types.Mask(
                    mask=lb_types.MaskData(im_bytes=entry["mask_bytes"]),
                    color=(255, 255, 255),
                ),
                classifications=[
                    lb_types.ClassificationAnnotation(
                        name="Taxon",
                        value=lb_types.Radio(
                            answer=lb_types.ClassificationAnswer(name=taxon_name)
                        ),
                    )
                ],
            )
        )

    return lb_types.Label(
        data={"global_key": global_key},
        annotations=annotations,
    )


def collect_rows(exports_dir: Path, stage: int, dataset_name: str | None) -> list[dict]:
    files = sorted(exports_dir.glob("2024_bci_*.json"))
    if stage == 1:
        files = files[:1]
    elif stage == 2:
        if not dataset_name:
            print("ERROR: --dataset required for stage 2")
            sys.exit(1)
        files = [exports_dir / f"{dataset_name}.json"]
        if not files[0].exists():
            print(f"ERROR: {files[0]} not found")
            sys.exit(1)

    rows = []
    for f in files:
        file_rows = json.load(open(f))
        rows.extend(file_rows)

    # Keep only rows that have labels
    labeled = []
    for row in rows:
        has_label = any(
            proj.get("labels")
            for proj in row.get("projects", {}).values()
        )
        if has_label:
            labeled.append(row)

    if stage == 1:
        labeled = labeled[:3]

    return labeled


def main():
    parser = argparse.ArgumentParser(description="Import ground truth labels into Project A.")
    parser.add_argument("--stage", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--dataset", type=str)
    args = parser.parse_args()

    load_dotenv()
    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    crosswalk_path = Path(config["folders"]["crosswalk"]) / "gbif_crosswalk.csv"
    species_list_path = Path(config["folders"]["species_list"]) / "bci_species_list.csv"

    api_key = os.environ["LABELBOX_API_KEY"]
    client = lb.Client(api_key=api_key)

    project_name = config["labelbox"]["project_a_name"]
    project = next((p for p in client.get_projects() if p.name == project_name), None)
    if project is None:
        print(f"ERROR: Project '{project_name}' not found. Run Phase 0f first.")
        sys.exit(1)

    crosswalk = load_crosswalk(crosswalk_path)
    wcvp_names = load_species_list(species_list_path)
    print(f"Crosswalk: {len(crosswalk)} GBIF->WCVP mappings")
    print(f"Species list: {len(wcvp_names)} WCVP taxa")

    rows = collect_rows(exports_dir, args.stage, args.dataset)
    print(f"\nStage {args.stage}: {len(rows)} labeled images to process")

    # Process images with concurrent mask downloads
    labels = []
    failed = 0
    for i, row in enumerate(rows, 1):
        gk = row["data_row"]["global_key"]
        print(f"  [{i}/{len(rows)}] {gk} ...", end=" ", flush=True)
        try:
            label = process_image(row, crosswalk, wcvp_names, api_key)
            if label:
                labels.append(label)
                print("OK")
            else:
                print("skipped (no valid taxa)")
        except Exception as e:
            print(f"ERROR: {e}")
            failed += 1

    print(f"\n{len(labels)} labels built, {failed} errors")
    if not labels:
        print("Nothing to import.")
        return

    # Import in batches
    print(f"\nImporting {len(labels)} labels in batches of {BATCH_SIZE} ...")
    total_ok = 0
    for i in range(0, len(labels), BATCH_SIZE):
        batch = labels[i:i + BATCH_SIZE]
        import_job = lb.LabelImport.create_from_objects(
            client=client,
            project_id=project.uid,
            name=f"gt_import_stage{args.stage}_{i}",
            labels=batch,
        )
        import_job.wait_until_done()
        errors = import_job.errors
        if errors:
            print(f"  Batch {i//BATCH_SIZE+1} errors: {errors[:3]}")
        else:
            total_ok += len(batch)
            print(f"  Batch {i//BATCH_SIZE+1}: {len(batch)} labels OK ({total_ok}/{len(labels)} total)")

    print(f"\nDone. {total_ok}/{len(labels)} labels imported into '{project_name}'.")
    if args.stage < 3:
        print(f"Review in Labelbox, then run stage {args.stage + 1}.")


if __name__ == "__main__":
    main()
