"""
Phase 0k — Import train/val/test split assignments into the combined dataset.

Reads the split CSV (global_key, image_url, mission, split), deduplicates on
global_key, and upserts the reserved 'split' enum metadata field on each data
row in the combined dataset.

The 'split' metadata field is a reserved org-level enum in Labelbox with options:
  train  → cko8sbscr0003h2dk04w86hof
  valid  → cko8sc2yr0004h2dk69aj5x63
  test   → cko8scbz70005h2dkastwhgqt

Images with no split assigned (empty string) are skipped — Labelbox treats
unset enum fields as unassigned.

Input:
  input/boxes/bci_images_for_plantnet_w_split.csv  (global_key, image_url, mission, split)

Output:
  Upserts 'split' enum metadata on data rows in 'BCI Workshop - Drone Photos'

Usage:
  python scripts/10_splits/10_import_splits.py --test   # first 5 rows only
  python scripts/10_splits/10_import_splits.py          # all rows
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import labelbox as lb
from dotenv import load_dotenv
import yaml

COMBINED_KEY_PREFIX = "comb_"
SPLIT_SCHEMA_ID = "cko8sbczn0002h2dkdaxb5kal"
SPLIT_OPTION_IDS = {
    "train": "cko8sbscr0003h2dk04w86hof",
    "valid": "cko8sc2yr0004h2dk69aj5x63",
    "test":  "cko8scbz70005h2dkastwhgqt",
}
BATCH_SIZE = 500
EXPORT_TIMEOUT_SEC = 300


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_splits(csv_path: Path) -> dict:
    """
    Load split CSV and return {global_key: split_value}.
    Deduplicates on global_key (all duplicates are consistent — same split).
    Rows with empty split are included with value '' so we know they exist.
    """
    splits = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            gk = row["global_key"]
            if gk not in splits:
                splits[gk] = row["split"].strip()
    return splits


def fetch_data_row_ids(client: lb.Client, dataset_name: str) -> dict:
    """
    Export combined dataset to get {combined_global_key: data_row_id}.
    """
    print(f"Finding dataset '{dataset_name}'...")
    dataset = next((d for d in client.get_datasets() if d.name == dataset_name), None)
    if dataset is None:
        sys.exit(f"ERROR: Dataset '{dataset_name}' not found.")
    print(f"  Found dataset ID: {dataset.uid}")

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
        pass  # Empty error stream — no errors

    key_to_id = {}
    export_task.get_buffered_stream(stream_type=lb.StreamType.RESULT).start(
        stream_handler=lambda output: key_to_id.update({
            output.json["data_row"]["global_key"]: output.json["data_row"]["id"]
        }) if "data_row" in output.json else None
    )
    print(f"  Exported {len(key_to_id)} data rows")
    return key_to_id


def main():
    parser = argparse.ArgumentParser(description="Import train/val/test splits into combined dataset.")
    parser.add_argument("--test", action="store_true", help="Process first 5 rows only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    split_csv = Path("input/boxes/bci_images_for_plantnet_w_split.csv")
    if not split_csv.exists():
        sys.exit(f"ERROR: {split_csv} not found.")

    combined_dataset_name = config["labelbox"]["combined_dataset_name"]

    # Step 1: Load splits
    print("Step 1 - Loading split assignments...")
    splits = load_splits(split_csv)
    assigned = {k: v for k, v in splits.items() if v in SPLIT_OPTION_IDS}
    skipped_empty = sum(1 for v in splits.values() if v == "")
    skipped_unknown = {k: v for k, v in splits.items() if v and v not in SPLIT_OPTION_IDS}
    print(f"  Unique global_keys: {len(splits)}")
    print(f"  With split assigned: {len(assigned)} (train={sum(1 for v in splits.values() if v=='train')}, "
          f"valid={sum(1 for v in splits.values() if v=='valid')}, "
          f"test={sum(1 for v in splits.values() if v=='test')})")
    print(f"  No split (will skip): {skipped_empty}")
    if skipped_unknown:
        print(f"  Unknown split values (will skip): {skipped_unknown}")

    # Step 2: Fetch data row IDs from Labelbox
    print("\nStep 2 - Fetching data row IDs from Labelbox...")
    client = lb.Client(api_key=api_key)
    key_to_id = fetch_data_row_ids(client, combined_dataset_name)

    # Step 3: Build upsert payloads
    print("\nStep 3 - Building upsert payloads...")
    updates = []
    skipped_no_dr = []

    items = list(assigned.items())
    if args.test:
        items = items[:5]
        print(f"  TEST MODE: processing {len(items)} rows only")

    for gk, split_val in items:
        combined_gk = COMBINED_KEY_PREFIX + gk
        dr_id = key_to_id.get(combined_gk)
        if dr_id is None:
            skipped_no_dr.append(gk)
            continue
        option_id = SPLIT_OPTION_IDS[split_val]
        updates.append(lb.DataRowMetadata(
            data_row_id=dr_id,
            fields=[lb.DataRowMetadataField(
                schema_id=SPLIT_SCHEMA_ID,
                value=option_id,
            )],
        ))

    print(f"  Rows to upsert: {len(updates)}")
    if skipped_no_dr:
        print(f"  Skipped (no matching data row in combined dataset): {len(skipped_no_dr)}")

    if not updates:
        sys.exit("No rows to update.")

    # Step 4: Upsert in batches
    print(f"\nStep 4 - Upserting 'split' metadata in batches of {BATCH_SIZE}...")
    mdo = client.get_data_row_metadata_ontology()
    total_ok = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        mdo.bulk_upsert(batch)
        total_ok += len(batch)
        print(f"  Batch {i // BATCH_SIZE + 1}: {len(batch)} rows updated ({total_ok}/{len(updates)} total)")

    print(f"\n{'=' * 50}")
    print("SUMMARY")
    print(f"{'=' * 50}")
    print(f"  Split rows in CSV:    {len(assigned)}")
    print(f"  Upserted to Labelbox: {total_ok}")
    print(f"  Skipped (no data row):{len(skipped_no_dr)}")
    print(f"  Skipped (no split):   {skipped_empty}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
