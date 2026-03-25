"""
Phase 0h — Export image list CSV for Pl@ntNet team.

Reads existing exported dataset JSONs and writes a CSV with one row per image.
No Labelbox API calls — read-only.

Output columns: global_key, image_url, mission

Usage:
  python scripts/05_export_for_plantnet/05_export_for_plantnet.py          # all 7717 rows
  python scripts/05_export_for_plantnet/05_export_for_plantnet.py --test   # first 2 rows only
"""

import argparse
import csv
import json
from pathlib import Path

import yaml


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Export image list CSV for Pl@ntNet team.")
    parser.add_argument("--test", action="store_true", help="Output first 2 rows only")
    args = parser.parse_args()

    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    output_dir = Path(config["folders"]["export_for_plantnet"])
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for json_file in sorted(exports_dir.glob("2024_bci_*.json")):
        for row in json.load(open(json_file)):
            dr = row["data_row"]
            mission = next(
                (m["value"] for m in row.get("metadata_fields", []) if m["schema_name"] == "mission"),
                "",
            )
            rows.append({
                "global_key": dr["global_key"],
                "image_url": dr["row_data"],
                "mission": mission,
            })
        if args.test and len(rows) >= 2:
            break

    if args.test:
        rows = rows[:2]

    out_file = output_dir / "bci_images_for_plantnet.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["global_key", "image_url", "mission"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_file}")


if __name__ == "__main__":
    main()
