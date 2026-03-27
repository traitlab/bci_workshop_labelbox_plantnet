"""
Phase 1a-single — Get Pl@ntNet single-species predictions for all BCI images.

Calls the Pl@ntNet /v2/identify/{project} endpoint (one call per image, 1 credit
each) and saves the top-N species results + organ predictions per image.

Uses the same center-crop (1280×1280) and disk-cache/resume pattern as
13a_get_embeddings.py. Safe to stop and resume at any time.

Config (config.yaml):
  plantnet.identify_url       — full API endpoint URL
  plantnet.identify_nb_results — number of top results to request (default 5)
  plantnet.identify_organs    — organ hint sent to API (default "auto")
  plantnet.identify_lang      — language for common names (default "en")

Input:
  output/05_export_for_plantnet/bci_images_for_plantnet.csv

Output:
  output/13_single_predictions/cache/<global_key>.json  — per-image cache
  output/13_single_predictions/predictions.json         — all results combined
  output/13_single_predictions/predictions_summary.json — run statistics

Usage:
  python scripts/13_single_predictions/13a_get_single_predictions.py --test
  python scripts/13_single_predictions/13a_get_single_predictions.py
  python scripts/13_single_predictions/13a_get_single_predictions.py --delay 1.0
"""

import argparse
import csv
import io
import json
import os
import sys
import time
from pathlib import Path

import requests
from PIL import Image
from dotenv import load_dotenv
import yaml

CROP_SIZE     = 1280
DEFAULT_DELAY = 0.5
MAX_RETRIES   = 3
API_TIMEOUT   = 60
BACKOFF       = [1, 5, 10]


class QuotaExceededError(Exception):
    pass


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_image_list(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def center_crop_jpeg(image_bytes: bytes) -> tuple[bytes, int, int, int | None]:
    """
    Center-crop to CROP_SIZE × CROP_SIZE and encode as JPEG.
    Returns (jpeg_bytes, orig_width, orig_height, crop_size_or_None).
    If image is smaller than CROP_SIZE in either dimension, returns as-is.
    """
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    w, h = img.size
    if w >= CROP_SIZE and h >= CROP_SIZE:
        left   = (w - CROP_SIZE) // 2
        top    = (h - CROP_SIZE) // 2
        img    = img.crop((left, top, left + CROP_SIZE, top + CROP_SIZE))
        crop_s = CROP_SIZE
    else:
        crop_s = None  # image smaller than crop target — send as-is

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue(), w, h, crop_s


def download_image(url: str, timeout: int = 30) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def call_identify_api(jpeg_bytes: bytes, filename: str, api_url: str,
                      api_key: str, nb_results: int,
                      organs: str, lang: str) -> dict:
    """
    Call /v2/identify endpoint. Returns parsed JSON response.
    Raises QuotaExceededError on HTTP 429, RuntimeError on other failures.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                api_url,
                files=[("images", (filename, io.BytesIO(jpeg_bytes), "image/jpeg"))],
                data={"organs": organs},
                params={
                    "api-key": api_key,
                    "nb-results": nb_results,
                    "no-reject": "true",
                    "include-related-images": "false",
                    "lang": lang,
                },
                timeout=API_TIMEOUT,
            )
            if resp.status_code == 429:
                raise QuotaExceededError(
                    f"API quota exceeded (HTTP 429). "
                    f"Remaining: {resp.headers.get('X-RateLimit-Remaining', 'unknown')}"
                )
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return resp.json()
        except QuotaExceededError:
            raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = BACKOFF[attempt]
                print(f"    Attempt {attempt + 1} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def parse_response(response: dict, global_key: str, image_url: str,
                   orig_width: int, orig_height: int,
                   crop_size: int | None) -> dict:
    """
    Extract top-N results and organ predictions from API response.
    Each result entry: {rank, score, scientific_name, family, genus, gbif_id, powo_id}
    Organs: list of unique organ strings from predictedOrgans (e.g. ["leaf", "flower"])
    """
    # Organs are in a separate top-level array, not nested per result
    organs_seen = []
    for po in response.get("predictedOrgans", []):
        organ = po.get("organ")
        if organ and organ not in organs_seen:
            organs_seen.append(organ)

    results = []
    for rank, r in enumerate(response.get("results", []), start=1):
        sp   = r.get("species", {})
        gbif = r.get("gbif",   {})
        powo = r.get("powo",   {})

        results.append({
            "rank":                rank,
            "score":               r.get("score"),
            "scientific_name":     sp.get("scientificNameWithoutAuthor"),
            "scientific_name_full": sp.get("scientificName"),
            "family":              sp.get("family", {}).get("scientificNameWithoutAuthor"),
            "genus":               sp.get("genus",  {}).get("scientificNameWithoutAuthor"),
            "gbif_id":             gbif.get("id"),
            "powo_id":             powo.get("id"),
        })

    best_match = results[0]["scientific_name"] if results else None

    return {
        "global_key":        global_key,
        "image_url":         image_url,
        "best_match":        best_match,
        "remaining_credits": response.get("remainingIdentificationRequests"),
        "original_width":    orig_width,
        "original_height":   orig_height,
        "crop_size":         crop_size,
        "results":           results,
        "organs":            organs_seen,
    }


def save_cache(cache_path: Path, entry: dict) -> None:
    tmp = cache_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(entry), encoding="utf-8")
    tmp.rename(cache_path)


def main():
    parser = argparse.ArgumentParser(
        description="Get Pl@ntNet single-species predictions for all BCI images."
    )
    parser.add_argument("--test",  action="store_true", help="Process 1 image only (verbose)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Delay between API calls in seconds (default {DEFAULT_DELAY})")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("PLANTNET_API_KEY")
    if not api_key:
        sys.exit("ERROR: PLANTNET_API_KEY not found in .env")

    pn_cfg     = config["plantnet"]
    api_url    = pn_cfg["identify_url"]
    nb_results = pn_cfg.get("identify_nb_results", 5)
    organs     = pn_cfg.get("identify_organs", "auto")
    lang       = pn_cfg.get("identify_lang", "en")

    images_csv  = Path(config["folders"]["export_for_plantnet"]) / "bci_images_for_plantnet.csv"
    output_dir  = Path(config["folders"]["single_predictions"])
    cache_dir   = output_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    print("Step 1 - Loading image list...")
    rows = load_image_list(images_csv)
    print(f"  {len(rows)} images")

    if args.test:
        rows = rows[:1]
        print("  TEST MODE: 1 image only")

    # Find already-cached images
    cached = {p.stem for p in cache_dir.glob("*.json")}
    to_process = [r for r in rows if r["global_key"] not in cached]
    print(f"\nStep 2 - Calling Pl@ntNet API ({api_url})...")
    print(f"  Already cached: {len(cached)}")
    print(f"  To process:     {len(to_process)}")

    if not to_process:
        print("  All images already cached — proceeding to assemble output.")
    else:
        print(f"  Delay: {args.delay}s between calls\n")

    ok = skipped = errors = 0
    last_remaining = None

    for i, row in enumerate(to_process):
        gk        = row["global_key"]
        image_url = row["image_url"]
        cache_path = cache_dir / f"{gk}.json"

        try:
            # Download image
            img_bytes = download_image(image_url)
            jpeg_bytes, orig_w, orig_h, crop_s = center_crop_jpeg(img_bytes)

            if args.test:
                print(f"  Image: {gk}")
                print(f"  Original size: {orig_w}×{orig_h}")
                print(f"  Crop size: {crop_s}")
                print(f"  JPEG size: {len(jpeg_bytes):,} bytes")

            # Call API
            response = call_identify_api(
                jpeg_bytes, f"{gk}.jpg", api_url, api_key, nb_results, organs, lang
            )

            if args.test:
                print(f"\n  Raw API response:")
                print(json.dumps(response, indent=2))

            # Parse and cache
            entry = parse_response(response, gk, image_url, orig_w, orig_h, crop_s)
            save_cache(cache_path, entry)
            ok += 1
            last_remaining = entry.get("remaining_credits")

            if args.test:
                print(f"\n  Parsed entry:")
                print(f"  Best match:  {entry['best_match']}")
                print(f"  Results:     {len(entry['results'])} species")
                for r in entry["results"]:
                    print(f"    #{r['rank']} {r['scientific_name']} "
                          f"(score={r['score']:.4f}, gbif={r['gbif_id']})")
                print(f"  Organs:      {entry['organs']}")
                print(f"  Credits remaining: {last_remaining}")
            elif (i + 1) % 100 == 0 or i == 0:
                cr = f", {last_remaining} credits remaining" if last_remaining else ""
                print(f"  [{i+1}/{len(to_process)}] {gk} — "
                      f"top={entry['best_match']}{cr}")

        except QuotaExceededError as e:
            print(f"\n  QUOTA EXCEEDED: {e}")
            print(f"  Processed {ok} images this run. Resume by re-running the script.")
            break
        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{len(to_process)}] ERROR {gk}: {e}")

        if i < len(to_process) - 1:
            time.sleep(args.delay)

    # Assemble final output from all cache files
    print("\nStep 3 - Assembling predictions.json from cache...")
    all_entries = []
    for p in sorted(cache_dir.glob("*.json")):
        all_entries.append(json.loads(p.read_text(encoding="utf-8")))

    out_path = output_dir / "predictions.json"
    out_path.write_text(json.dumps(all_entries, indent=2), encoding="utf-8")

    summary = {
        "total_cached":       len(all_entries),
        "processed_this_run": ok,
        "skipped_cached":     skipped,
        "errors_this_run":    errors,
        "api_url":            api_url,
        "nb_results":         nb_results,
        "organs":             organs,
        "crop_size":          CROP_SIZE,
        "last_credits_remaining": last_remaining,
        "test_mode":          args.test,
    }
    summary_path = output_dir / "predictions_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"  {len(all_entries)} predictions written to {out_path}")
    print(f"\n{'=' * 55}")
    print("SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Total in cache:      {len(all_entries)}")
    print(f"  Processed this run:  {ok}")
    print(f"  Errors this run:     {errors}")
    if last_remaining is not None:
        print(f"  Credits remaining:   {last_remaining}")
    print(f"  Output:              {out_path}")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
