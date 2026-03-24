"""
Phase 0b — Validate that Alliance Canada image URLs are still accessible.

Checks row_data URLs and attachment URLs (IMAGE and HTML types) from all
exported dataset JSON files. Uses HEAD requests for efficiency.

By default samples 5 random rows per dataset. Use --all to check every row.

Usage:
  python scripts/01_validate_urls/01_validate_urls.py
  python scripts/01_validate_urls/01_validate_urls.py --all
"""

import argparse
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml

TIMEOUT = 10  # seconds per request
WORKERS = 10  # concurrent requests
SAMPLE_PER_DATASET = 5


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def collect_urls(exports_dir: Path, sample: bool) -> list[dict]:
    """Collect URLs to check from all exported dataset JSON files."""
    urls = []
    for json_file in sorted(exports_dir.glob("2024_bci_*.json")):
        rows = json.load(open(json_file))
        if sample:
            rows = random.sample(rows, min(SAMPLE_PER_DATASET, len(rows)))
        for row in rows:
            dr = row["data_row"]
            urls.append({"url": dr["row_data"], "type": "image", "global_key": dr["global_key"], "dataset": json_file.stem})
            for att in row.get("attachments", []):
                if att.get("type") in ("IMAGE", "HTML") and att.get("value", "").startswith("http"):
                    urls.append({"url": att["value"], "type": f"attachment:{att['type']}", "global_key": dr["global_key"], "dataset": json_file.stem})
    return urls


def check_url(entry: dict) -> dict:
    """Send a HEAD request and return status."""
    try:
        resp = requests.head(entry["url"], timeout=TIMEOUT, allow_redirects=True)
        entry["status_code"] = resp.status_code
        entry["ok"] = resp.status_code == 200
        entry["error"] = None
    except requests.RequestException as e:
        entry["status_code"] = None
        entry["ok"] = False
        entry["error"] = str(e)
    return entry


def main():
    parser = argparse.ArgumentParser(description="Validate Alliance Canada image URLs.")
    parser.add_argument("--all", action="store_true", help="Check all rows (default: sample 5 per dataset)")
    args = parser.parse_args()

    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    output_dir = Path(config["folders"]["exports"])  # write results alongside exports

    sample = not args.all
    urls = collect_urls(exports_dir, sample)
    total = len(urls)
    print(f"Checking {total} URLs ({'sampled' if sample else 'all'}) with {WORKERS} workers ...")

    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(check_url, entry): entry for entry in urls}
        done = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            done += 1
            if not result["ok"]:
                failed.append(result)
                print(f"  FAIL [{result['status_code'] or result['error']}] {result['url']}")
            if done % 50 == 0:
                print(f"  {done}/{total} checked ...")

    ok_count = total - len(failed)
    print(f"\nResults: {ok_count}/{total} OK, {len(failed)} failed")

    out_file = output_dir / "url_validation.json"
    with open(out_file, "w") as f:
        json.dump({"total": total, "ok": ok_count, "failed_count": len(failed), "failed": failed}, f, indent=2)

    if failed:
        print(f"\nFailed URLs written to {out_file}")
        raise SystemExit(1)
    else:
        print("All URLs accessible.")


if __name__ == "__main__":
    main()
