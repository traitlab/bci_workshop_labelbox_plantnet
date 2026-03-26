"""
Add 'original_labelbox_url' metadata to combined dataset rows.

Builds the URL from existing exported JSON files (no re-export needed):
  https://app.labelbox.com/projects/{project_id}/data-rows/{data_row_id}

Uses the first real label project found per data row (from label_projects in config).
Data rows with no labels in any real project get an empty string.

Idempotent: creates the metadata schema if it doesn't exist, then upserts values.

Usage:
  python scripts/04_combined_dataset/04b_add_original_url_metadata.py
"""

import json
import os
from pathlib import Path

import labelbox as lb
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from dotenv import load_dotenv
import yaml

LABELBOX_APP_BASE = "https://app.labelbox.com"
METADATA_FIELD = "original_labelbox_url"
BATCH_SIZE = 500


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def ensure_metadata_schema(client: lb.Client, field_name: str) -> str:
    ontology = client.get_data_row_metadata_ontology()
    for field in ontology.fields:
        if field.name == field_name:
            print(f"  Schema '{field_name}' already exists: {field.uid}")
            return field.uid
    new_field = ontology.create_schema(name=field_name, kind=DataRowMetadataKind.string)
    print(f"  Created schema '{field_name}': {new_field.uid}")
    return new_field.uid


def build_url_map(exports_dir: Path, label_projects: list[str]) -> dict[str, str]:
    """
    Returns {original_global_key: url} for all data rows that have labels
    in a real label project.
    """
    label_project_set = set(label_projects)
    url_map = {}

    for json_file in sorted(exports_dir.glob("2024_bci_*.json")):
        rows = json.load(open(json_file))
        for row in rows:
            dr = row["data_row"]
            original_key = dr["global_key"]
            original_dr_id = dr["id"]

            # Find first real label project for this row
            for proj_id, proj in row.get("projects", {}).items():
                if proj.get("name") in label_project_set and proj.get("labels"):
                    url = f"{LABELBOX_APP_BASE}/projects/{proj_id}/data-rows/{original_dr_id}"
                    url_map[original_key] = url
                    break
            # Rows with no labels in real projects get empty string (handled below)

    return url_map


def main():
    load_dotenv()
    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    combined_name = config["labelbox"]["combined_dataset_name"]
    label_projects = config["labelbox"]["label_projects"]

    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])

    print("Step 1 - Ensuring metadata schema ...")
    schema_id = ensure_metadata_schema(client, METADATA_FIELD)

    print("\nStep 2 - Building URL map from exported JSONs ...")
    url_map = build_url_map(exports_dir, label_projects)
    print(f"  {len(url_map)} data rows have a source URL")

    print(f"\nStep 3 - Fetching combined dataset data rows ...")
    dataset = next((d for d in client.get_datasets() if d.name == combined_name), None)
    if dataset is None:
        print(f"  ERROR: dataset '{combined_name}' not found")
        return

    data_rows = list(dataset.data_rows())
    print(f"  {len(data_rows)} data rows in combined dataset")

    # Build upsert payloads
    updates = []
    for dr in data_rows:
        # dr.global_key is the comb_ prefixed key; strip prefix to get original
        original_key = dr.global_key.removeprefix("comb_")
        url = url_map.get(original_key, "")
        if url:
            updates.append({
                "data_row_id": dr.uid,
                "fields": [{"schema_id": schema_id, "value": url}],
            })

    print(f"  {len(updates)} rows to update with URL metadata")

    print(f"\nStep 4 - Upserting metadata in batches of {BATCH_SIZE} ...")
    mdo = client.get_data_row_metadata_ontology()
    total_ok = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        results = mdo.bulk_upsert([
            lb.DataRowMetadata(
                data_row_id=u["data_row_id"],
                fields=[lb.DataRowMetadataField(schema_id=f["schema_id"], value=f["value"])
                        for f in u["fields"]],
            )
            for u in batch
        ])
        total_ok += len(batch)
        print(f"  Batch {i//BATCH_SIZE+1}: {len(batch)} rows updated ({total_ok}/{len(updates)} total)")

    print(f"\nDone. {total_ok} rows updated with '{METADATA_FIELD}' metadata.")


if __name__ == "__main__":
    main()
