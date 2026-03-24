"""
Phase 0c — Build GBIF backbone ↔ WCVP crosswalk CSV.

For each unique taxon ID found in the exported annotation JSON files:
  1. Look up the GBIF backbone record to get canonical name and rank
  2. Roll up VARIETY taxa to their parent species
  3. Resolve SYNONYM taxa to their accepted taxon
  4. Match canonical name to the WCVP dataset on GBIF to get the WCVP gbifId
     (which is what Pl@ntNet returns as predictions)

Output:
  output/01_crosswalk/gbif_crosswalk.csv
  output/01_crosswalk/crosswalk_summary.json

Usage:
  python scripts/02_crosswalk/02_build_crosswalk.py
"""

import csv
import json
import time
from pathlib import Path

import requests
import yaml

GBIF_API = "https://api.gbif.org/v1/species"
WCVP_DATASET_KEY = "f382f0ce-323a-4091-bb9f-add557f3a9a2"
REQUEST_DELAY = 0.5  # seconds between GBIF API calls


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Step 1 — Extract unique taxon IDs from exported JSON files
# ---------------------------------------------------------------------------

def extract_taxa(exports_dir: Path) -> dict:
    """
    Returns {gbif_id: {"original_name": str, "annotation_count": int}}
    gbif_id is a string. original_name is blank string for unannotated masks.
    """
    taxa = {}  # gbif_id -> {original_name, annotation_count}
    empty_count = 0

    for json_file in sorted(exports_dir.glob("2024_bci_*.json")):
        rows = json.load(open(json_file))
        for row in rows:
            for proj in row.get("projects", {}).values():
                for label in proj.get("labels", []):
                    for obj in label.get("annotations", {}).get("objects", []):
                        for cls in obj.get("classifications", []):
                            answers = cls.get("checklist_answers", [])
                            if not answers:
                                empty_count += 1
                                continue
                            for answer in answers:
                                gbif_id = str(answer.get("value", "")).strip()
                                name = answer.get("name", "").strip()
                                if not gbif_id:
                                    empty_count += 1
                                    continue
                                if gbif_id not in taxa:
                                    taxa[gbif_id] = {"original_name": name, "annotation_count": 0}
                                taxa[gbif_id]["annotation_count"] += 1

    print(f"  Found {len(taxa)} unique taxon IDs, {empty_count} unannotated masks")
    return taxa


# ---------------------------------------------------------------------------
# Step 2 — Look up GBIF backbone record, handle rank and synonym resolution
# ---------------------------------------------------------------------------

def gbif_lookup(gbif_id: str) -> dict:
    """Fetch species record from GBIF backbone."""
    url = f"{GBIF_API}/{gbif_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def resolve_taxon(gbif_id: str) -> dict:
    """
    Resolve a GBIF backbone ID to a canonical name ready for WCVP matching.
    Handles: synonym resolution, VARIETY rollup to species.

    Returns dict with keys: canonical_name, rank, original_rank, gbif_status, notes_parts (list)
    """
    notes = []
    data = gbif_lookup(gbif_id)
    time.sleep(REQUEST_DELAY)

    original_rank = data.get("rank", "")
    status = data.get("taxonomicStatus", "")

    # Resolve synonym chains
    if status in ("SYNONYM", "HETEROTYPIC_SYNONYM", "HOMOTYPIC_SYNONYM", "PROPARTE_SYNONYM"):
        accepted_key = data.get("acceptedKey") or data.get("accepted", {}).get("key")
        if accepted_key:
            notes.append(f"synonym resolved to {accepted_key}")
            data = gbif_lookup(str(accepted_key))
            time.sleep(REQUEST_DELAY)
            status = data.get("taxonomicStatus", "")

    rank = data.get("rank", "")
    canonical_name = data.get("canonicalName", "")

    # Roll up VARIETY to parent species
    if rank == "VARIETY":
        parent_key = data.get("parentKey")
        if parent_key:
            notes.append("rolled up from VARIETY to species")
            parent_data = gbif_lookup(str(parent_key))
            time.sleep(REQUEST_DELAY)
            canonical_name = parent_data.get("canonicalName", canonical_name)
            rank = parent_data.get("rank", rank)
        else:
            notes.append("VARIETY — no parent found, using variety name")

    if rank == "SUBSPECIES":
        notes.append("subspecies — verify Pl@ntNet coverage")

    return {
        "canonical_name": canonical_name,
        "rank": rank,
        "original_rank": original_rank,
        "gbif_status": status,
        "notes_parts": notes,
    }


# ---------------------------------------------------------------------------
# Step 3 — Match canonical name to WCVP dataset on GBIF
# ---------------------------------------------------------------------------

def match_wcvp(canonical_name: str) -> dict:
    """
    Try to match a canonical name to the WCVP dataset on GBIF.
    Returns: wcvp_gbif_id, match_type, match_confidence
    """
    if not canonical_name:
        return {"wcvp_gbif_id": "", "match_type": "", "match_confidence": ""}

    # Primary: restrict to WCVP dataset
    params = {"name": canonical_name, "datasetKey": WCVP_DATASET_KEY}
    resp = requests.get(f"{GBIF_API}/match", params=params, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    time.sleep(REQUEST_DELAY)

    match_type = result.get("matchType", "NONE")
    if match_type != "NONE" and result.get("usageKey"):
        return {
            "wcvp_gbif_id": str(result["usageKey"]),
            "match_type": match_type,
            "match_confidence": str(result.get("confidence", "")),
        }

    # Fallback: unrestricted match, accept only if result is from WCVP
    params_fallback = {"name": canonical_name}
    resp2 = requests.get(f"{GBIF_API}/match", params=params_fallback, timeout=10)
    resp2.raise_for_status()
    result2 = resp2.json()
    time.sleep(REQUEST_DELAY)

    match_type2 = result2.get("matchType", "NONE")
    if match_type2 != "NONE" and result2.get("datasetKey") == WCVP_DATASET_KEY and result2.get("usageKey"):
        return {
            "wcvp_gbif_id": str(result2["usageKey"]),
            "match_type": match_type2,
            "match_confidence": str(result2.get("confidence", "")),
        }

    return {"wcvp_gbif_id": "", "match_type": "NONE", "match_confidence": ""}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    exports_dir = Path(config["folders"]["exports"])
    output_dir = Path(config["folders"]["crosswalk"])
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Step 1 — Extracting unique taxon IDs from exported JSON files ...")
    taxa = extract_taxa(exports_dir)

    print(f"\nStep 2+3 — Resolving {len(taxa)} taxa via GBIF API ...")
    rows = []
    for i, (gbif_id, meta) in enumerate(sorted(taxa.items(), key=lambda x: -x[1]["annotation_count"]), 1):
        print(f"  [{i}/{len(taxa)}] {gbif_id}  {meta['original_name']}")
        try:
            resolved = resolve_taxon(gbif_id)
            wcvp = match_wcvp(resolved["canonical_name"])
        except Exception as e:
            print(f"    ERROR: {e}")
            resolved = {"canonical_name": "", "rank": "", "original_rank": "", "gbif_status": "ERROR", "notes_parts": [str(e)]}
            wcvp = {"wcvp_gbif_id": "", "match_type": "", "match_confidence": ""}

        notes = "; ".join(resolved["notes_parts"])
        if not wcvp["wcvp_gbif_id"] and wcvp["match_type"] == "NONE":
            if notes:
                notes += "; no WCVP match"
            else:
                notes = "no WCVP match"

        rows.append({
            "gbif_backbone_id": gbif_id,
            "original_name": meta["original_name"],
            "canonical_name": resolved["canonical_name"],
            "rank": resolved["rank"],
            "original_rank": resolved["original_rank"],
            "gbif_status": resolved["gbif_status"],
            "wcvp_gbif_id": wcvp["wcvp_gbif_id"],
            "match_type": wcvp["match_type"],
            "match_confidence": wcvp["match_confidence"],
            "annotation_count": meta["annotation_count"],
            "notes": notes,
        })

    # Write CSV
    csv_file = output_dir / "gbif_crosswalk.csv"
    fieldnames = ["gbif_backbone_id", "original_name", "canonical_name", "rank", "original_rank",
                  "gbif_status", "wcvp_gbif_id", "match_type", "match_confidence", "annotation_count", "notes"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nCrosswalk CSV written to {csv_file}")

    # Write summary
    from collections import Counter
    summary = {
        "total_taxa": len(rows),
        "by_rank": dict(Counter(r["rank"] for r in rows)),
        "by_original_rank": dict(Counter(r["original_rank"] for r in rows)),
        "by_gbif_status": dict(Counter(r["gbif_status"] for r in rows)),
        "by_match_type": dict(Counter(r["match_type"] for r in rows)),
        "wcvp_matched": sum(1 for r in rows if r["wcvp_gbif_id"]),
        "wcvp_unmatched": sum(1 for r in rows if not r["wcvp_gbif_id"]),
        "subspecies_flagged": sum(1 for r in rows if "subspecies" in r["notes"]),
        "variety_rolled_up": sum(1 for r in rows if "VARIETY" in r["notes"]),
    }
    summary_file = output_dir / "crosswalk_summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Summary written to {summary_file}")
    print(f"\nResults: {summary['wcvp_matched']} matched, {summary['wcvp_unmatched']} unmatched")
    print(f"  by match_type: {summary['by_match_type']}")
    print(f"  by rank: {summary['by_rank']}")
    if summary["subspecies_flagged"]:
        print(f"  WARNING: {summary['subspecies_flagged']} subspecies taxa flagged — verify Pl@ntNet coverage")


if __name__ == "__main__":
    main()
