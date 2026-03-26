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

Mask PNGs are cached to disk under output/07_gt_masks_cache/ so re-runs skip
already-downloaded masks. Labels already imported into Labelbox are also skipped.

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

MASK_WORKERS = 30     # concurrent mask downloads
BATCH_SIZE = 100      # labels per import batch
MASK_RETRY = 3        # retries per mask download


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_crosswalk(crosswalk_path: Path) -> dict:
    xwalk = {}
    for row in csv.DictReader(open(crosswalk_path)):
        if row["wcvp_gbif_id"]:
            xwalk[row["gbif_backbone_id"]] = row["wcvp_gbif_id"]
    return xwalk


def load_species_list(species_list_path: Path) -> dict:
    return {
        row["wcvp_gbif_id"]: row["wcvp_canonical_name"]
        for row in csv.DictReader(open(species_list_path))
        if row["wcvp_gbif_id"]
    }


def url_to_cache_path(cache_dir: Path, url: str) -> Path:
    h = hashlib.md5(url.encode()).hexdigest()
    return cache_dir / f"{h}.png"


def download_mask(url: str, api_key: str, cache_dir: Path) -> bytes | None:
    """Download a mask PNG with disk caching and retries."""
    cache_path = url_to_cache_path(cache_dir, url)
    if cache_path.exists():
        return cache_path.read_bytes()
    headers = {"Authorization": f"Bearer {api_key}"}
    for attempt in range(MASK_RETRY):
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
    arr = np.array(img)
    return int((arr[:, :, 3] > 0).sum())


def collect_mask_urls(row: dict, crosswalk: dict) -> list[dict]:
    """Extract all mask URLs + taxon info from an exported row."""
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
                wcvp_ids = [crosswalk[a["value"]] for a in answers if crosswalk.get(a["value"])]
                if wcvp_ids:
                    entries.append({"url": mask_url, "wcvp_ids": wcvp_ids})
    return entries


def build_label(row: dict, crosswalk: dict, wcvp_names: dict,
                api_key: str, cache_dir: Path) -> lb_types.Label | None:
    global_key = "comb_" + row["data_row"]["global_key"]
    mask_entries_info = collect_mask_urls(row, crosswalk)
    if not mask_entries_info:
        return None

    # Deduplicate by URL: multiple objects can share the same mask PNG. Merge their taxa.
    url_to_wcvp_ids: dict[str, list[str]] = defaultdict(list)
    for info in mask_entries_info:
        for wid in info["wcvp_ids"]:
            if wid not in url_to_wcvp_ids[info["url"]]:
                url_to_wcvp_ids[info["url"]].append(wid)

    # Download each unique mask PNG once, count pixels for all associated taxa.
    # Only one annotation per PNG to avoid "Found duplicate mask" errors.
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
                    "pixel_count": px_per_taxon,
                })

    if not mask_entries:
        return None

    taxa_present = set(taxa_pixel_counts.keys())
    dominant_wcvp_id = max(taxa_pixel_counts, key=taxa_pixel_counts.get)
    dominant_name = wcvp_names.get(dominant_wcvp_id, dominant_wcvp_id)

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
                    lb_types.ClassificationAnswer(name=wcvp_names[wid])
                    for wid in sorted(taxa_present)
                    if wid in wcvp_names
                ]
            ),
        ),
    ]

    for entry in mask_entries:
        wcvp_id = entry["wcvp_id"]
        if wcvp_id not in wcvp_names:
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
                            answer=[lb_types.ClassificationAnswer(name=wcvp_names[wcvp_id])]
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
        rows.extend(json.load(open(f)))

    labeled = [
        row for row in rows
        if any(proj.get("labels") for proj in row.get("projects", {}).values())
    ]

    if stage == 1:
        labeled = labeled[:3]
    return labeled


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--dataset", type=str)
    args = parser.parse_args()

    load_dotenv()
    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    crosswalk_path = Path(config["folders"]["crosswalk"]) / "gbif_crosswalk.csv"
    species_list_path = Path(config["folders"]["species_list"]) / "bci_species_list.csv"
    cache_dir = Path(config["folders"]["output"]) / "07_gt_masks_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.environ["LABELBOX_API_KEY"]
    client = lb.Client(api_key=api_key)

    project_name = config["labelbox"]["project_a_name"]
    project = next((p for p in client.get_projects() if p.name == project_name), None)
    if project is None:
        print(f"ERROR: Project '{project_name}' not found.")
        sys.exit(1)

    crosswalk = load_crosswalk(crosswalk_path)
    wcvp_names = load_species_list(species_list_path)

    rows = collect_rows(exports_dir, args.stage, args.dataset)
    print(f"Stage {args.stage}: {len(rows)} labeled images")

    # Find already-imported global keys to skip
    print("Fetching already-imported global keys ...")
    imported_keys = set()
    for label in project.labels():
        imported_keys.add(label.data_row().global_key)
    print(f"  {len(imported_keys)} already imported, will skip")

    rows_to_process = [
        r for r in rows
        if ("comb_" + r["data_row"]["global_key"]) not in imported_keys
    ]
    print(f"  {len(rows_to_process)} remaining to process\n")

    if not rows_to_process:
        print("Nothing to do — all already imported.")
        return

    # Process images in parallel
    labels = []
    failed = 0
    done = 0

    def process(row):
        return build_label(row, crosswalk, wcvp_names, api_key, cache_dir)

    with ThreadPoolExecutor(max_workers=MASK_WORKERS) as executor:
        futures = {executor.submit(process, row): row for row in rows_to_process}
        for future in as_completed(futures):
            done += 1
            row = futures[future]
            gk = row["data_row"]["global_key"]
            try:
                label = future.result()
                if label:
                    labels.append(label)
                    status = "OK"
                else:
                    status = "skipped"
            except Exception as e:
                status = f"ERROR: {e}"
                failed += 1
            if done % 50 == 0 or done <= 10:
                print(f"  [{done}/{len(rows_to_process)}] {gk} ... {status}")

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
            name=f"gt_s{args.stage}_{i}",
            labels=batch,
        )
        import_job.wait_till_done()
        errors = import_job.errors
        if errors:
            print(f"  Batch {i//BATCH_SIZE+1} errors: {errors[:3]}")
        else:
            total_ok += len(batch)
            print(f"  Batch {i//BATCH_SIZE+1}: {len(batch)} OK ({total_ok}/{len(labels)} total)")

    print(f"\nDone. {total_ok}/{len(labels)} labels imported into '{project_name}'.")
    if args.stage < 3:
        print(f"Review in Labelbox, then run stage {args.stage + 1}.")


if __name__ == "__main__":
    main()
