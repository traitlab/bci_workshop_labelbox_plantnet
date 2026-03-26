"""
Phase 2b -- Upload Pl@ntNet embeddings to Labelbox combined dataset.

Reads the embeddings.json produced by Phase 2a, maps global keys to Labelbox
data row IDs by exporting the combined dataset, writes an NDJSON file, then
uploads it to a custom Labelbox embedding (creating it if it doesn't exist).

After upload, similarity search in Labelbox Catalog becomes available for
the combined BCI dataset (requires >= 1000 vectors, which we have).

Output:
  output/08_embeddings/embeddings_upload.ndjson  (NDJSON for Labelbox import)
  output/08_embeddings/upload_summary.json       (run statistics)

Usage:
  python scripts/08_embeddings/08b_upload_embeddings.py --test    # 5 rows only
  python scripts/08_embeddings/08b_upload_embeddings.py           # all 7717 rows
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import labelbox as lb
import yaml
from dotenv import load_dotenv

EMBEDDING_NAME = "PlantNet-v7.4-1280px"
EMBEDDING_DIMS = 768
COMBINED_KEY_PREFIX = "comb_"
EXPORT_TIMEOUT_SEC = 300   # max seconds to wait for dataset export


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_embeddings(path: Path) -> dict:
    """Load embeddings.json → dict keyed by global_key (without comb_ prefix)."""
    with open(path) as f:
        raw = json.load(f)
    result = {}
    for entry in raw:
        gk = entry["global_key"]
        result[gk] = entry["embedding"]
    print(f"Loaded {len(result)} embeddings from {path}")
    return result


def export_combined_dataset(client: lb.Client, dataset_name: str) -> dict:
    """
    Export the combined dataset to get data row IDs.
    Returns dict: combined_global_key -> data_row_id
    """
    print(f"Finding dataset '{dataset_name}'...")
    datasets = [d for d in client.get_datasets() if d.name == dataset_name]
    if not datasets:
        sys.exit(f"ERROR: Dataset '{dataset_name}' not found in Labelbox.")
    dataset = datasets[0]
    print(f"  Found dataset ID: {dataset.uid}")

    print("Exporting data row IDs from combined dataset...")
    export_task = dataset.export(params={
        "attachments": False,
        "metadata_fields": False,
        "data_row_details": True,
        "embeddings": False,
        "labels": False,
    })
    export_task.wait_till_done(timeout_seconds=EXPORT_TIMEOUT_SEC)

    # Check for streaming errors
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


def get_or_create_embedding(client: lb.Client, name: str, dims: int):
    """Return existing custom embedding with this name, or create a new one."""
    existing = client.get_embeddings()
    for emb in existing:
        if emb.name == name and emb.custom:
            print(f"  Found existing embedding '{name}' (id={emb.id}, dims={emb.dims})")
            if emb.dims != dims:
                sys.exit(f"ERROR: Existing embedding has dims={emb.dims}, expected {dims}.")
            return emb
    print(f"  Creating new custom embedding '{name}' with dims={dims}...")
    emb = client.create_embedding(name=name, dims=dims)
    print(f"  Created embedding id={emb.id}")
    return emb


def write_ndjson(ndjson_path: Path, embeddings: dict, key_to_id: dict, test: bool) -> tuple:
    """
    Write NDJSON file for Labelbox import.
    Each line: {"id": "<data_row_id>", "vector": [...]}

    Returns (rows_written, skipped_no_id, skipped_no_embedding).
    """
    rows_written = 0
    skipped_no_id = []
    skipped_no_embedding = []

    items = list(embeddings.items())
    if test:
        items = items[:5]
        print(f"  TEST MODE: writing {len(items)} rows only")

    with open(ndjson_path, "w") as f:
        for gk, vector in items:
            combined_gk = COMBINED_KEY_PREFIX + gk
            dr_id = key_to_id.get(combined_gk)
            if dr_id is None:
                skipped_no_id.append(gk)
                continue
            if not vector:
                skipped_no_embedding.append(gk)
                continue
            line = json.dumps({"id": dr_id, "vector": vector})
            f.write(line + "\n")
            rows_written += 1

    return rows_written, skipped_no_id, skipped_no_embedding


def main():
    parser = argparse.ArgumentParser(description="Upload Pl@ntNet embeddings to Labelbox.")
    parser.add_argument("--test", action="store_true",
                        help="Upload only 5 rows (for testing)")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    output_dir = Path(config["folders"]["embeddings"])
    embeddings_path = output_dir / "embeddings.json"
    ndjson_path = output_dir / "embeddings_upload.ndjson"
    summary_path = output_dir / "upload_summary.json"
    combined_dataset_name = config["labelbox"]["combined_dataset_name"]

    if not embeddings_path.exists():
        sys.exit(f"ERROR: {embeddings_path} not found. Run Phase 2a first.")

    # Step 1: Load embeddings
    embeddings = load_embeddings(embeddings_path)

    # Step 2: Connect to Labelbox
    client = lb.Client(api_key=api_key)

    # Step 3: Export combined dataset to get data row IDs
    key_to_id = export_combined_dataset(client, combined_dataset_name)

    # Step 4: Write NDJSON
    print(f"\nWriting NDJSON to {ndjson_path}...")
    rows_written, skipped_no_id, skipped_no_embedding = write_ndjson(
        ndjson_path, embeddings, key_to_id, args.test
    )
    print(f"  Rows written:          {rows_written}")
    if skipped_no_id:
        print(f"  Skipped (no data row): {len(skipped_no_id)}")
    if skipped_no_embedding:
        print(f"  Skipped (no vector):   {len(skipped_no_embedding)}")

    if rows_written == 0:
        sys.exit("ERROR: No rows to upload. Check that the combined dataset exists and embeddings are loaded.")

    # Step 5: Get or create the Labelbox embedding
    print(f"\nSetting up Labelbox embedding '{EMBEDDING_NAME}'...")
    embedding = get_or_create_embedding(client, EMBEDDING_NAME, EMBEDDING_DIMS)

    # Step 6: Upload
    print(f"\nUploading {rows_written} vectors to Labelbox...")
    print("  (This may take a few minutes — vectors are ingested asynchronously)")

    batch_count = 0
    def on_batch(resp):
        nonlocal batch_count
        batch_count += 1
        print(f"  Batch {batch_count} uploaded: {resp}")

    embedding.import_vectors_from_file(str(ndjson_path), callback=on_batch)
    print("Upload complete.")

    # Step 7: Check vector count (may lag slightly, retry a few times)
    print("\nChecking imported vector count (may take a moment to sync)...")
    imported_count = 0
    for attempt in range(6):
        time.sleep(5)
        imported_count = embedding.get_imported_vector_count()
        print(f"  Attempt {attempt + 1}: {imported_count} vectors imported")
        if imported_count >= rows_written:
            break

    # Step 8: Summary
    summary = {
        "embedding_id": embedding.id,
        "embedding_name": embedding.name,
        "embedding_dims": EMBEDDING_DIMS,
        "rows_in_embeddings_json": len(embeddings),
        "rows_written_to_ndjson": rows_written,
        "skipped_no_data_row_id": len(skipped_no_id),
        "skipped_no_vector": len(skipped_no_embedding),
        "imported_vector_count": imported_count,
        "test_mode": args.test,
        "timestamp": datetime.now().isoformat(),
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Embedding name:    {embedding.name}")
    print(f"  Embedding ID:      {embedding.id}")
    print(f"  Rows uploaded:     {rows_written}")
    print(f"  Vectors confirmed: {imported_count}")
    print(f"  Skipped:           {len(skipped_no_id) + len(skipped_no_embedding)}")
    print(f"  NDJSON:            {ndjson_path}")
    print(f"  Summary:           {summary_path}")
    print(f"{'=' * 60}")
    if imported_count < rows_written:
        print(f"\nNOTE: Labelbox ingests vectors asynchronously. Run again later to check final count.")
        print(f"  python scripts/08_embeddings/08b_upload_embeddings.py --check-count")


if __name__ == "__main__":
    main()
