"""
Phase 1-multi step d — Compute mean-pooled embeddings from multi-species
tile predictions and upload to Labelbox as custom embedding 'PlantNet-v7.4-multi'.

For each image:
  1. Read per_tiles_embeddings from the survey JSON (768-dim per tile)
  2. Mean-pool across all tiles -> single 768-dim vector
  3. L2-normalize

Upload to Labelbox combined dataset using the same NDJSON pattern as
08b_upload_embeddings.py.

Usage:
  python scripts/14_multi_predictions/14d_upload_multi_embeddings.py --test
  python scripts/14_multi_predictions/14d_upload_multi_embeddings.py
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import labelbox as lb
import yaml
from dotenv import load_dotenv

EMBEDDING_DIMS = 768
COMBINED_KEY_PREFIX = "comb_"
EXPORT_TIMEOUT_SEC = 300


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def compute_embeddings(multi_dir: Path, test: bool = False) -> list:
    """
    Compute mean-pooled, L2-normalized embeddings from multi-species JSONs.
    Returns list of {global_key, embedding}.
    """
    json_files = sorted(multi_dir.glob("*.JPG.json"))
    if test:
        json_files = json_files[:5]

    print(f"  Processing {len(json_files)} files ...")
    results = []
    skipped = 0

    for i, jf in enumerate(json_files):
        global_key = jf.name.replace(".json", "")

        try:
            with open(jf, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  WARNING: Skipping {jf.name} — parse error: {e}")
            skipped += 1
            continue

        tiles = data.get("results", {}).get("per_tiles_embeddings", [])
        if not tiles:
            skipped += 1
            continue

        # Mean-pool
        n = len(tiles)
        pooled = [0.0] * EMBEDDING_DIMS
        for tile in tiles:
            emb = tile.get("embeddings", [])
            if len(emb) != EMBEDDING_DIMS:
                continue
            for j in range(EMBEDDING_DIMS):
                pooled[j] += emb[j]

        for j in range(EMBEDDING_DIMS):
            pooled[j] /= n

        # L2-normalize
        norm = math.sqrt(sum(v * v for v in pooled))
        if norm > 0:
            pooled = [v / norm for v in pooled]

        results.append({"global_key": global_key, "embedding": pooled})

        if (i + 1) % 1000 == 0:
            print(f"    {i + 1}/{len(json_files)} ...")

    print(f"  Computed {len(results)} embeddings ({skipped} skipped)")
    return results


def export_combined_dataset(client: lb.Client, dataset_name: str) -> dict:
    """Export combined dataset -> {combined_global_key: data_row_id}."""
    print(f"  Finding dataset '{dataset_name}' ...")
    datasets = [d for d in client.get_datasets() if d.name == dataset_name]
    if not datasets:
        sys.exit(f"ERROR: Dataset '{dataset_name}' not found.")
    dataset = datasets[0]
    print(f"  Found dataset ID: {dataset.uid}")

    print("  Exporting data row IDs ...")
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

    key_to_id = {}
    export_task.get_buffered_stream(stream_type=lb.StreamType.RESULT).start(
        stream_handler=lambda output: key_to_id.update({
            output.json["data_row"]["global_key"]: output.json["data_row"]["id"]
        }) if "data_row" in output.json else None
    )

    print(f"  Exported {len(key_to_id)} data rows")
    return key_to_id


def get_or_create_embedding(client: lb.Client, name: str, dims: int):
    """Return existing custom embedding or create new one."""
    existing = client.get_embeddings()
    for emb in existing:
        if emb.name == name and emb.custom:
            print(f"  Found existing embedding '{name}' (id={emb.id}, dims={emb.dims})")
            if emb.dims != dims:
                sys.exit(f"ERROR: Existing embedding has dims={emb.dims}, expected {dims}.")
            return emb
    print(f"  Creating new custom embedding '{name}' with dims={dims} ...")
    emb = client.create_embedding(name=name, dims=dims)
    print(f"  Created embedding id={emb.id}")
    return emb


def main():
    parser = argparse.ArgumentParser(
        description="Compute + upload multi-species mean-pooled embeddings."
    )
    parser.add_argument("--test", action="store_true",
                        help="Process first 5 files only")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    multi_dir = Path(config["plantnet"]["multi_predictions_dir"])
    output_dir = Path(config["folders"]["multi_predictions"])
    output_dir.mkdir(parents=True, exist_ok=True)
    embedding_name = config["plantnet"]["multi_embedding_name"]
    combined_dataset_name = config["labelbox"]["combined_dataset_name"]

    ndjson_path = output_dir / "multi_embeddings_upload.ndjson"
    summary_path = output_dir / "multi_embeddings_summary.json"

    # Step 1: Compute embeddings
    print("Step 1 — Computing mean-pooled embeddings ...")
    embeddings = compute_embeddings(multi_dir, test=args.test)

    if not embeddings:
        sys.exit("ERROR: No embeddings computed.")

    # Save intermediate JSON
    emb_json_path = output_dir / "multi_embeddings.json"
    with open(emb_json_path, "w") as f:
        json.dump(embeddings, f)
    print(f"  Saved to {emb_json_path}")

    # Step 2: Connect to Labelbox
    print("\nStep 2 — Connecting to Labelbox ...")
    client = lb.Client(api_key=api_key)

    # Step 3: Export combined dataset
    print("\nStep 3 — Exporting combined dataset ...")
    key_to_id = export_combined_dataset(client, combined_dataset_name)

    # Step 4: Write NDJSON
    print(f"\nStep 4 — Writing NDJSON to {ndjson_path} ...")
    rows_written = 0
    skipped_no_id = 0

    with open(ndjson_path, "w") as f:
        for entry in embeddings:
            gk = entry["global_key"]
            combined_gk = COMBINED_KEY_PREFIX + gk
            dr_id = key_to_id.get(combined_gk)
            if dr_id is None:
                skipped_no_id += 1
                continue
            line = json.dumps({"id": dr_id, "vector": entry["embedding"]})
            f.write(line + "\n")
            rows_written += 1

    print(f"  Rows written:          {rows_written}")
    if skipped_no_id:
        print(f"  Skipped (no data row): {skipped_no_id}")

    if rows_written == 0:
        sys.exit("ERROR: No rows to upload.")

    # Step 5: Get or create embedding
    print(f"\nStep 5 — Setting up Labelbox embedding '{embedding_name}' ...")
    embedding = get_or_create_embedding(client, embedding_name, EMBEDDING_DIMS)

    # Step 6: Upload
    print(f"\nStep 6 — Uploading {rows_written} vectors ...")
    batch_count = 0
    def on_batch(resp):
        nonlocal batch_count
        batch_count += 1
        print(f"  Batch {batch_count} uploaded: {resp}")

    embedding.import_vectors_from_file(str(ndjson_path), callback=on_batch)
    print("  Upload complete.")

    # Step 7: Check vector count
    print("\nStep 7 — Checking imported vector count ...")
    imported_count = 0
    for attempt in range(6):
        time.sleep(5)
        imported_count = embedding.get_imported_vector_count()
        print(f"  Attempt {attempt + 1}: {imported_count} vectors imported")
        if imported_count >= rows_written:
            break

    # Step 8: Summary
    summary = {
        "embedding_name": embedding_name,
        "embedding_id": embedding.id,
        "embedding_dims": EMBEDDING_DIMS,
        "total_files": len(list(multi_dir.glob("*.JPG.json"))),
        "embeddings_computed": len(embeddings),
        "rows_written": rows_written,
        "skipped_no_data_row": skipped_no_id,
        "imported_vector_count": imported_count,
        "test_mode": args.test,
        "timestamp": datetime.now().isoformat(),
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 60}")
    print("SUMMARY — MULTI-SPECIES EMBEDDINGS")
    print(f"{'=' * 60}")
    print(f"  Embedding name:    {embedding_name}")
    print(f"  Embedding ID:      {embedding.id}")
    print(f"  Rows uploaded:     {rows_written}")
    print(f"  Vectors confirmed: {imported_count}")
    print(f"  Summary:           {summary_path}")
    print(f"{'=' * 60}")
    if imported_count < rows_written:
        print(f"\nNOTE: Labelbox ingests vectors asynchronously. Count may increase.")


if __name__ == "__main__":
    main()
