"""
Phase 2a -- Get Pl@ntNet embeddings for BCI drone images.

Downloads each image from Alliance Canada, center-crops to 1280x1280,
sends to the Pl@ntNet /v2/embeddings endpoint, and saves the returned
embedding vectors to disk.

Output:
  output/08_embeddings/embeddings.json          (all embeddings, for Phase 2b)
  output/08_embeddings/cache/<global_key>.json   (per-image cache)
  output/08_embeddings/embeddings_summary.json   (run statistics)

Usage:
  python scripts/08_embeddings/08_get_embeddings.py --test          # 1 image, verbose
  python scripts/08_embeddings/08_get_embeddings.py                  # all 7717 images
  python scripts/08_embeddings/08_get_embeddings.py --delay 1.0     # custom delay
"""

import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv
from PIL import Image

CROP_SIZE = 1280
JPEG_QUALITY = 90
MAX_RETRIES = 3
DEFAULT_DELAY = 0.5
IMAGE_DOWNLOAD_TIMEOUT = 30
API_TIMEOUT = 60
COMBINED_KEY_PREFIX = "comb_"


class QuotaExceededError(Exception):
    pass


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def load_images_csv(csv_path: Path) -> list:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def download_image(url: str) -> bytes:
    resp = requests.get(url, timeout=IMAGE_DOWNLOAD_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def center_crop(image_bytes: bytes) -> tuple:
    """Center-crop image to CROP_SIZE x CROP_SIZE. Returns (jpeg_bytes, metadata)."""
    img = Image.open(io.BytesIO(image_bytes))
    w, h = img.size
    meta = {"original_width": w, "original_height": h, "crop_size": None}

    if w >= CROP_SIZE and h >= CROP_SIZE:
        left = (w - CROP_SIZE) // 2
        top = (h - CROP_SIZE) // 2
        img = img.crop((left, top, left + CROP_SIZE, top + CROP_SIZE))
        meta["crop_size"] = CROP_SIZE
    else:
        print(f"    WARNING: image is {w}x{h}, smaller than {CROP_SIZE}x{CROP_SIZE} — sending as-is")

    if img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY)
    return buf.getvalue(), meta


def call_embeddings_api(jpeg_bytes: bytes, filename: str, api_key: str, api_url: str) -> dict:
    """Send cropped JPEG to /v2/embeddings. Returns raw JSON response."""
    files = [("image", (filename, io.BytesIO(jpeg_bytes), "image/jpeg"))]
    params = {"api-key": api_key}
    resp = requests.post(api_url, files=files, params=params, timeout=API_TIMEOUT)

    if resp.status_code == 429:
        raise QuotaExceededError(f"Pl@ntNet API quota exceeded (HTTP 429): {resp.text}")
    resp.raise_for_status()
    return resp.json()


def extract_embedding(api_response: dict) -> tuple:
    """
    Extract embedding vector and version from API response.
    Returns (embedding_list, plantnet_version).

    NOTE: Response structure is discovered via --test run. This function
    tries common key names and falls back to inspection.
    """
    # Try common key names for embedding vector
    embedding = None
    for key in ("embedding", "embeddings", "vector"):
        if key in api_response:
            val = api_response[key]
            if isinstance(val, list) and len(val) > 0:
                # Could be a list of floats, or a list of dicts with embeddings
                if isinstance(val[0], (int, float)):
                    embedding = val
                elif isinstance(val[0], dict) and "embeddings" in val[0]:
                    # Tile-style response: mean-pool
                    import math
                    dims = len(val[0]["embeddings"])
                    mean_vec = [0.0] * dims
                    for tile in val:
                        for j, v in enumerate(tile["embeddings"]):
                            mean_vec[j] += v
                    mean_vec = [v / len(val) for v in mean_vec]
                    norm = math.sqrt(sum(v * v for v in mean_vec))
                    if norm > 0:
                        mean_vec = [v / norm for v in mean_vec]
                    embedding = mean_vec
                break

    if embedding is None:
        raise ValueError(
            f"Could not find embedding in API response. Keys: {list(api_response.keys())}. "
            f"Run with --test to inspect the full response."
        )

    # Try common key names for version
    version = None
    for key in ("version", "plantnet_version", "model_version", "model"):
        if key in api_response and isinstance(api_response[key], str):
            version = api_response[key]
            break

    return embedding, version


def load_cache_entry(cache_dir: Path, global_key: str):
    cache_file = cache_dir / f"{global_key}.json"
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)
    return None


def save_cache_entry(cache_dir: Path, global_key: str, entry: dict):
    cache_file = cache_dir / f"{global_key}.json"
    tmp_file = cache_file.with_suffix(".tmp")
    with open(tmp_file, "w") as f:
        json.dump(entry, f)
    tmp_file.replace(cache_file)


def assemble_embeddings(cache_dir: Path, images: list) -> list:
    """Read all cache files in CSV order."""
    result = []
    for row in images:
        entry = load_cache_entry(cache_dir, row["global_key"])
        if entry is not None:
            result.append(entry)
    return result


def main():
    parser = argparse.ArgumentParser(description="Get Pl@ntNet embeddings for BCI drone images.")
    parser.add_argument("--test", action="store_true", help="Process 1 image only, verbose output")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Delay between API calls in seconds (default: {DEFAULT_DELAY})")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("PLANTNET_API_KEY")
    if not api_key:
        sys.exit("ERROR: PLANTNET_API_KEY not found in .env")

    api_url = config["plantnet"]["embeddings_api_url"]
    output_dir = Path(config["folders"]["embeddings"])
    csv_path = Path(config["folders"]["export_for_plantnet"]) / "bci_images_for_plantnet.csv"
    cache_dir = output_dir / "cache"
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    images = load_images_csv(csv_path)
    print(f"Loaded {len(images)} images from {csv_path}")

    if args.test:
        images = images[:1]
        print("TEST MODE: processing 1 image only\n")

    # Count cached
    cached_count = sum(1 for img in images if (cache_dir / f"{img['global_key']}.json").exists())
    remaining = len(images) - cached_count
    print(f"Total: {len(images)}, cached: {cached_count}, remaining: {remaining}")

    if remaining == 0 and not args.test:
        print("All images already cached. Assembling final output...")
        all_embeddings = assemble_embeddings(cache_dir, images)
        embeddings_path = output_dir / "embeddings.json"
        with open(embeddings_path, "w") as f:
            json.dump(all_embeddings, f)
        print(f"Wrote {len(all_embeddings)} embeddings to {embeddings_path}")
        return

    # Process loop
    plantnet_version = None
    processed = 0
    skipped = 0
    failed_images = []

    print(f"\n{'=' * 60}")
    print(f"Processing images (delay={args.delay}s between API calls)")
    print(f"{'=' * 60}\n")

    for i, row in enumerate(images, 1):
        gk = row["global_key"]
        url = row["image_url"]

        # Check cache
        cached = load_cache_entry(cache_dir, gk)
        if cached is not None:
            if args.test:
                print(f"  [CACHED] {gk}")
                print(f"  Embedding dims: {len(cached['embedding'])}")
                print(f"  First 10 values: {cached['embedding'][:10]}")
            if plantnet_version is None:
                plantnet_version = cached.get("plantnet_version")
            skipped += 1
            continue

        # Process with retries
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Step 1: Download
                if args.test:
                    print(f"  Downloading: {url}")
                raw_bytes = download_image(url)
                if args.test:
                    print(f"  Downloaded: {len(raw_bytes)} bytes")

                # Step 2: Center crop
                jpeg_bytes, crop_meta = center_crop(raw_bytes)
                if args.test:
                    print(f"  Original: {crop_meta['original_width']}x{crop_meta['original_height']}")
                    print(f"  Crop size: {crop_meta['crop_size']}")
                    print(f"  JPEG size after crop: {len(jpeg_bytes)} bytes")

                # Step 3: Call API
                if args.test:
                    print(f"  Calling API: {api_url}")
                api_response = call_embeddings_api(jpeg_bytes, gk, api_key, api_url)
                if args.test:
                    print(f"  API response keys: {list(api_response.keys())}")
                    # Print truncated response for inspection
                    resp_str = json.dumps(api_response, indent=2)
                    if len(resp_str) > 2000:
                        print(f"  API response (truncated):\n{resp_str[:2000]}...")
                    else:
                        print(f"  API response:\n{resp_str}")

                # Step 4: Extract embedding
                embedding, version = extract_embedding(api_response)
                if plantnet_version is None and version:
                    plantnet_version = version

                if args.test:
                    print(f"  Embedding dims: {len(embedding)}")
                    print(f"  First 10 values: {embedding[:10]}")
                    print(f"  Pl@ntNet version: {version}")

                # Step 5: Build entry and cache
                entry = {
                    "global_key": gk,
                    "combined_global_key": COMBINED_KEY_PREFIX + gk,
                    "image_url": url,
                    "embedding": embedding,
                    "original_width": crop_meta["original_width"],
                    "original_height": crop_meta["original_height"],
                    "crop_size": crop_meta["crop_size"],
                    "plantnet_version": version,
                }
                save_cache_entry(cache_dir, gk, entry)
                processed += 1
                success = True
                break

            except QuotaExceededError as e:
                print(f"\n  QUOTA EXCEEDED after {processed} images processed.")
                print(f"  {e}")
                print(f"  Re-run later to resume from image {i}.")
                # Assemble what we have
                all_embeddings = assemble_embeddings(cache_dir, images)
                embeddings_path = output_dir / "embeddings.json"
                with open(embeddings_path, "w") as f:
                    json.dump(all_embeddings, f)
                print(f"  Saved {len(all_embeddings)} embeddings so far to {embeddings_path}")
                sys.exit(1)

            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait = [1, 5, 10][attempt - 1]
                    print(f"  Attempt {attempt}/{MAX_RETRIES} failed for {gk}: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"  FAILED after {MAX_RETRIES} attempts: {gk}: {e}")
                    failed_images.append({"global_key": gk, "error": str(e)})

        # Progress report
        if not args.test and (i % 100 == 0 or i == len(images)):
            elapsed_cached = skipped
            print(f"  [{i}/{len(images)}] processed={processed} cached={elapsed_cached} failed={len(failed_images)}")

        # Delay between API calls (skip if cached or test)
        if success and not args.test and i < len(images):
            time.sleep(args.delay)

    # Assemble final output
    print(f"\n{'=' * 60}")
    print("Assembling final output")
    print(f"{'=' * 60}")

    all_embeddings = assemble_embeddings(cache_dir, images)

    embeddings_path = output_dir / "embeddings.json"
    with open(embeddings_path, "w") as f:
        json.dump(all_embeddings, f)
    print(f"Wrote {len(all_embeddings)} embeddings to {embeddings_path}")

    # Write summary
    dims = len(all_embeddings[0]["embedding"]) if all_embeddings else None
    summary = {
        "total_images": len(images),
        "embeddings_saved": len(all_embeddings),
        "processed_this_run": processed,
        "cached_from_previous": skipped,
        "failed": len(failed_images),
        "failed_images": failed_images,
        "plantnet_version": plantnet_version,
        "crop_size": CROP_SIZE,
        "embedding_dims": dims,
        "timestamp": datetime.now().isoformat(),
    }

    summary_path = output_dir / "embeddings_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total images:      {len(images)}")
    print(f"  Embeddings saved:  {len(all_embeddings)}")
    print(f"  Processed now:     {processed}")
    print(f"  From cache:        {skipped}")
    print(f"  Failed:            {len(failed_images)}")
    print(f"  Embedding dims:    {dims}")
    print(f"  Pl@ntNet version:  {plantnet_version}")
    print(f"  Output:            {embeddings_path}")
    print(f"  Summary:           {summary_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
