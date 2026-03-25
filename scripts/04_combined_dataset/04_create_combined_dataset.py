"""
Phase 0e — Create combined BCI dataset in Labelbox.

Creates a new dataset called (from config) 'BCI Workshop - Drone Photos' with new
data rows pointing to the same Alliance Canada image URLs as the existing 2024_bci*
datasets.

Global keys are prefixed with 'comb_' to avoid conflicts with existing data rows.
The original global key is stored in a metadata field 'original_global_key'.

What is imported per data row:
  - row_data       : Alliance Canada image URL (unchanged)
  - global_key     : 'comb_' + original global_key
  - external_id    : same as new global_key
  - metadata_fields: all existing metadata fields + 'original_global_key'
  - attachments    : all existing attachments

Labels, embeddings, and predictions are imported in later phases.

Safety: read-only against existing datasets. Only creates a new dataset.

Three-stage safety protocol:
  Stage 1: demo — import first 3 rows from first dataset only
  Stage 2: one dataset — import all rows from one named dataset
  Stage 3: all  — import all rows from all 2024_bci* datasets

Usage:
  python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 1
  python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 2 --dataset 2024_bci_20240911_bcifairchild_wptsendero_m3e
  python scripts/04_combined_dataset/04_create_combined_dataset.py --stage 3
"""

import argparse
import json
import os
import sys
from pathlib import Path

import labelbox as lb
from dotenv import load_dotenv
import yaml

GLOBAL_KEY_PREFIX = "comb_"
ORIGINAL_KEY_FIELD = "original_global_key"
BATCH_SIZE = 500  # data rows per upsert call


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def build_data_row(row: dict) -> dict:
    """Convert an exported JSON row to a Labelbox DataRow upload dict."""
    dr = row["data_row"]
    original_key = dr["global_key"]
    new_key = GLOBAL_KEY_PREFIX + original_key

    # Metadata: carry over existing fields + add original_global_key
    metadata = [
        {"schema_id": m["schema_id"], "value": m["value"]}
        for m in row.get("metadata_fields", [])
    ]
    # original_global_key is a free-text field — store as attachment-style note
    # We'll add it as a metadata field using the same schema infrastructure.
    # Since we can't guarantee the schema exists, we store it in a dedicated
    # metadata field. If the schema doesn't exist yet, Labelbox will error —
    # handled below by creating the schema if needed.
    # For now, embed original_key in the external_id comment via a second metadata entry.
    # Simpler: just store in a second metadata field with schema lookup at runtime.
    # We handle schema creation in main().

    attachments = [
        {"type": att["type"], "value": att["value"], "name": att.get("name", "")}
        for att in row.get("attachments", [])
        if att.get("value", "").startswith("http")
    ]

    return {
        "row_data": dr["row_data"],
        "global_key": new_key,
        "external_id": new_key,
        "metadata_fields": metadata,
        "attachments": attachments,
        # Store original_global_key — schema_id filled in main() after lookup
        "_original_key": original_key,
    }


def ensure_metadata_schema(client: lb.Client, field_name: str) -> str | None:
    """
    Look up or create a CustomMetadataString schema for field_name.
    Returns the schema_id, or None if it already exists under a different kind.
    """
    ontology_builder = client.get_data_row_metadata_ontology()
    for field in ontology_builder.fields:
        if field.name == field_name:
            return field.uid
    # Create it
    from labelbox.schema.data_row_metadata import DataRowMetadataKind
    new_field = ontology_builder.create_schema(
        name=field_name,
        kind=DataRowMetadataKind.string,
    )
    return new_field.uid


def collect_rows(exports_dir: Path, stage: int, dataset_name: str | None) -> list[dict]:
    """Load exported JSON rows according to stage."""
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
        if stage == 1:
            file_rows = file_rows[:3]
        rows.extend(file_rows)
    return rows


def upload_batch(dataset: lb.Dataset, batch: list[dict], original_key_schema_id: str):
    """Upload a batch of data rows to the dataset."""
    upload_dicts = []
    for r in batch:
        metadata = list(r["metadata_fields"])
        if original_key_schema_id:
            metadata.append({"schema_id": original_key_schema_id, "value": r["_original_key"]})
        upload_dicts.append({
            "row_data": r["row_data"],
            "global_key": r["global_key"],
            "external_id": r["external_id"],
            "metadata_fields": metadata,
            "attachments": r["attachments"],
        })
    task = dataset.create_data_rows(upload_dicts)
    task.wait_till_done()
    errors = task.errors
    if errors:
        print(f"  ERRORS in batch: {errors}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Create combined BCI dataset in Labelbox.")
    parser.add_argument("--stage", type=int, required=True, choices=[1, 2, 3],
                        help="1=demo (3 rows), 2=one dataset, 3=all datasets")
    parser.add_argument("--dataset", type=str, help="Dataset name for stage 2 (without .json)")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    combined_name = config["labelbox"]["combined_dataset_name"]

    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])

    # Ensure original_global_key metadata schema exists
    print("Checking metadata schema ...")
    original_key_schema_id = ensure_metadata_schema(client, ORIGINAL_KEY_FIELD)
    print(f"  '{ORIGINAL_KEY_FIELD}' schema_id: {original_key_schema_id}")

    # Collect rows
    print(f"\nCollecting rows for stage {args.stage} ...")
    rows = collect_rows(exports_dir, args.stage, args.dataset)
    data_rows = [build_data_row(r) for r in rows]
    print(f"  {len(data_rows)} data rows to import")

    # Get or create the combined dataset
    print(f"\nLooking up dataset '{combined_name}' ...")
    dataset = next((d for d in client.get_datasets() if d.name == combined_name), None)
    if dataset is None:
        print(f"  Creating new dataset '{combined_name}' ...")
        dataset = client.create_dataset(name=combined_name)
        print(f"  Created: {dataset.uid}")
    else:
        print(f"  Found existing dataset: {dataset.uid}")

    # Fetch already-uploaded global keys to skip duplicates on re-runs
    print("  Fetching existing global keys ...")
    existing_keys = {dr.global_key for dr in dataset.data_rows()}
    print(f"  {len(existing_keys)} rows already in dataset")
    new_data_rows = [r for r in data_rows if r["global_key"] not in existing_keys]
    skipped = len(data_rows) - len(new_data_rows)
    if skipped:
        print(f"  Skipping {skipped} already-uploaded rows")
    print(f"  {len(new_data_rows)} new rows to upload")

    if not new_data_rows:
        print("\nNothing to upload — all rows already present.")
        return

    # Upload in batches
    print(f"\nUploading {len(new_data_rows)} rows in batches of {BATCH_SIZE} ...")
    total_ok = 0
    for i in range(0, len(new_data_rows), BATCH_SIZE):
        batch = new_data_rows[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}: rows {i+1}-{min(i+BATCH_SIZE, len(new_data_rows))} ...")
        ok = upload_batch(dataset, batch, original_key_schema_id)
        if ok:
            total_ok += len(batch)
            print(f"    OK ({total_ok}/{len(new_data_rows)} total)")

    print(f"\nDone. {total_ok}/{len(new_data_rows)} new rows uploaded to '{combined_name}' ({len(existing_keys) + total_ok} total).")
    if args.stage < 3:
        print(f"\nReview the dataset in Labelbox, then run stage {args.stage + 1}.")


if __name__ == "__main__":
    main()
