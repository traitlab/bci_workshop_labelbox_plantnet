"""
Phase 0a — Export all 2024_bci datasets + labels (masks) to JSON.

Three-stage safety protocol:
  Stage 1: Run against the demo dataset only. Review output, confirm with human.
  Stage 2: Run against a single 2024_bci* dataset. Review, confirm with human.
  Stage 3: Run against all 2024_bci* datasets.

Usage:
  python scripts/00_export/00_export_datasets.py --stage 1
  python scripts/00_export/00_export_datasets.py --stage 2 --dataset "2024_bci_XXXX"
  python scripts/00_export/00_export_datasets.py --stage 3
"""

import argparse
import json
import os
import time
from pathlib import Path

import labelbox as lb
import yaml
from dotenv import load_dotenv

load_dotenv()


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def slim_row(row: dict, label_projects: list[str]) -> dict:
    """Strip fields not needed for re-import. Keep: image URL, global key, attachments,
    metadata, and labels (annotations only) from allowed projects only."""
    dr = row["data_row"]
    slimmed = {
        "data_row": {
            "id": dr["id"],
            "global_key": dr["global_key"],
            "row_data": dr["row_data"],  # image URL
        },
        "attachments": row.get("attachments", []),
        "metadata_fields": row.get("metadata_fields", []),
        "projects": {},
    }
    for proj_id, proj in row.get("projects", {}).items():
        if proj["name"] not in label_projects:
            continue
        slimmed["projects"][proj_id] = {
            "name": proj["name"],
            "labels": [
                {
                    "label_kind": lbl["label_kind"],
                    "version": lbl["version"],
                    "annotations": lbl["annotations"],
                }
                for lbl in proj.get("labels", [])
            ],
        }
    return slimmed


def export_dataset(client, dataset, output_dir: Path, label_projects: list[str]) -> dict:
    """Export a single dataset's data rows and labels to JSON. Read-only."""
    print(f"  Exporting dataset: {dataset.name} ...")

    export_task = dataset.export(
        params={
            "attachments": True,
            "metadata_fields": True,
            "embeddings": False,
            "data_row_details": False,
            "project_details": False,
            "label_details": False,
            "all_projects": True,
            "all_model_runs": False,
            "performance_details": False,
            "interpolated_frames": False,
        }
    )
    export_task.wait_till_done()

    # Check for streaming errors (stream may be empty if no errors)
    try:
        errors = []
        export_task.get_buffered_stream(stream_type=lb.StreamType.ERRORS).start(
            stream_handler=lambda output: errors.append(output.json)
        )
        if errors:
            raise RuntimeError(f"Export errors for {dataset.name}: {errors}")
    except ValueError:
        pass  # Empty error stream — no errors

    rows = []
    export_task.get_buffered_stream(stream_type=lb.StreamType.RESULT).start(
        stream_handler=lambda output: rows.append(slim_row(output.json, label_projects))
    )

    safe_name = dataset.name.replace("/", "_").replace(" ", "_")
    out_file = output_dir / f"{safe_name}.json"
    with open(out_file, "w") as f:
        json.dump(rows, f, indent=2)

    print(f"    -> {len(rows)} data rows saved to {out_file}")
    return {"dataset_name": dataset.name, "dataset_id": dataset.uid, "row_count": len(rows), "file": str(out_file)}


def main():
    parser = argparse.ArgumentParser(description="Export BCI Labelbox datasets (read-only).")
    parser.add_argument("--stage", type=int, required=True, choices=[1, 2, 3],
                        help="Safety stage: 1=demo, 2=single dataset, 3=all datasets")
    parser.add_argument("--dataset", type=str, default=None,
                        help="Dataset name for stage 2 (required for --stage 2)")
    args = parser.parse_args()

    if args.stage == 2 and not args.dataset:
        parser.error("--dataset is required for --stage 2")

    config = load_config()
    output_dir = Path(config["folders"]["exports"])
    output_dir.mkdir(parents=True, exist_ok=True)

    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])

    # Determine which datasets to export
    if args.stage == 1:
        print(f"STAGE 1: Exporting demo dataset only: '{config['labelbox']['demo_dataset_name']}'")
        all_datasets = list(client.get_datasets())
        target = [d for d in all_datasets if d.name == config["labelbox"]["demo_dataset_name"]]
        if not target:
            raise RuntimeError(f"Demo dataset not found: {config['labelbox']['demo_dataset_name']}")

    elif args.stage == 2:
        print(f"STAGE 2: Exporting single dataset: '{args.dataset}'")
        all_datasets = list(client.get_datasets())
        target = [d for d in all_datasets if d.name == args.dataset]
        if not target:
            raise RuntimeError(f"Dataset not found: {args.dataset}")

    else:  # stage 3
        prefix = config["labelbox"]["dataset_prefix"]
        print(f"STAGE 3: Exporting all datasets with prefix '{prefix}'")
        all_datasets = list(client.get_datasets())
        target = [d for d in all_datasets if d.name.startswith(prefix)]
        if not target:
            raise RuntimeError(f"No datasets found with prefix: {prefix}")
        print(f"  Found {len(target)} datasets.")

    label_projects = config["labelbox"]["label_projects"]
    print(f"  Keeping labels from projects: {label_projects}")

    # Export
    summary = []
    for dataset in target:
        result = export_dataset(client, dataset, output_dir, label_projects)
        summary.append(result)
        time.sleep(1)  # be polite to the API

    # Write summary
    summary_file = output_dir / f"export_summary_stage{args.stage}.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nDone. Exported {len(summary)} dataset(s). Summary: {summary_file}")
    print("\nNEXT STEP: Review the output, then proceed to the next stage or Phase 0b.")


if __name__ == "__main__":
    main()
