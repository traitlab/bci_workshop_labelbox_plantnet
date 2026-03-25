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
CACHE_FILE = Path("output/01_crosswalk/gbif_api_cache.json")


def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.load(open(CACHE_FILE))
    return {}


def save_cache(cache: dict):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(cache, open(CACHE_FILE, "w"), indent=2)


_cache: dict = {}


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
    """Fetch species record from GBIF backbone (cached)."""
    key = f"lookup:{gbif_id}"
    if key in _cache:
        return _cache[key]
    url = f"{GBIF_API}/{gbif_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = data
    save_cache(_cache)
    return data


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

# Families that have been synonymised and need remapping to their current WCVP name
FAMILY_REMAPS = {
    "Hippocrateaceae": "Celastraceae",  # subsumed into Celastraceae under modern taxonomy
}


def match_wcvp(canonical_name: str) -> dict:
    """
    Find the WCVP record for a canonical name using /species/search restricted to
    the WCVP dataset. The /species/match endpoint does NOT support datasetKey filtering
    (it always searches the GBIF backbone), so we use /species/search instead.

    Returns: wcvp_gbif_id, match_type, match_confidence
    """
    if not canonical_name:
        return {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": "", "match_type": "", "match_confidence": ""}

    search_name = FAMILY_REMAPS.get(canonical_name, canonical_name)
    remapped = search_name != canonical_name
    key = f"wcvp:{canonical_name}"  # cache key always uses original name
    if key in _cache:
        results = _cache[key]
    else:
        params = {"q": search_name, "datasetKey": WCVP_DATASET_KEY, "limit": 5}
        resp = requests.get(f"{GBIF_API}/search", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        _cache[key] = results
        save_cache(_cache)
        time.sleep(REQUEST_DELAY)

    note = f"remapped {canonical_name} -> {search_name}" if remapped else ""

    def resolve_accepted(r: dict) -> dict | None:
        """If r is a SYNONYM, follow acceptedKey to get the accepted WCVP record."""
        if r.get("taxonomicStatus") == "ACCEPTED":
            return r
        accepted_key = r.get("acceptedKey")
        if not accepted_key:
            return None
        accepted_data = gbif_lookup(str(accepted_key))
        return accepted_data

    # Prefer ACCEPTED exact match; if only synonyms, follow acceptedKey
    exact_matches = [r for r in results if r.get("canonicalName", "").lower() == search_name.lower()]
    accepted = next((r for r in exact_matches if r.get("taxonomicStatus") == "ACCEPTED"), None)
    if not accepted and exact_matches:
        accepted = resolve_accepted(exact_matches[0])
        if accepted:
            note = (note + "; synonym resolved in WCVP").lstrip("; ")
    if accepted:
        return {
            "wcvp_gbif_id": str(accepted.get("key", accepted.get("usageKey", ""))),
            "wcvp_canonical_name": accepted.get("canonicalName", ""),
            "wcvp_status": accepted.get("taxonomicStatus", ""),
            "match_type": "EXACT",
            "match_confidence": "100",
            "remap_note": note,
        }

    # Fuzzy fallback: accept first result with same genus, prefer ACCEPTED
    if results:
        q_parts = canonical_name.lower().split()
        same_genus = [r for r in results if r.get("canonicalName", "").lower().split()[:1] == q_parts[:1]]
        r = next((r for r in same_genus if r.get("taxonomicStatus") == "ACCEPTED"), same_genus[0] if same_genus else None)
        if r:
            if r.get("taxonomicStatus") != "ACCEPTED":
                r = resolve_accepted(r) or r
            return {
                "wcvp_gbif_id": str(r.get("key", r.get("usageKey", ""))),
                "wcvp_canonical_name": r.get("canonicalName", ""),
                "wcvp_status": r.get("taxonomicStatus", ""),
                "match_type": "FUZZY",
                "match_confidence": "",
                "remap_note": "",
            }

    return {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": "", "match_type": "NONE", "match_confidence": "", "remap_note": ""}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global _cache
    _cache = load_cache()
    if _cache:
        print(f"  Loaded {len(_cache)} cached API responses")

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
            wcvp = {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": "", "match_type": "", "match_confidence": ""}

        notes_parts = resolved["notes_parts"][:]
        if wcvp.get("remap_note"):
            notes_parts.append(wcvp["remap_note"])
        if not wcvp["wcvp_gbif_id"] and wcvp["match_type"] == "NONE":
            notes_parts.append("no WCVP match")
        notes = "; ".join(notes_parts)

        rows.append({
            "gbif_backbone_id": gbif_id,
            "original_name": meta["original_name"],
            "original_rank": resolved["original_rank"],
            "gbif_canonical_name": resolved["canonical_name"],
            "rank": resolved["rank"],
            "gbif_backbone_status": resolved["gbif_status"],
            "wcvp_gbif_id": wcvp["wcvp_gbif_id"],
            "wcvp_canonical_name": wcvp["wcvp_canonical_name"],
            "wcvp_status": wcvp["wcvp_status"],
            "match_type": wcvp["match_type"],
            "match_confidence": wcvp["match_confidence"],
            "annotation_count": meta["annotation_count"],
            "notes": notes,
        })

    # Write CSV
    csv_file = output_dir / "gbif_crosswalk.csv"
    fieldnames = ["gbif_backbone_id", "original_name", "original_rank", "gbif_canonical_name", "rank",
                  "gbif_backbone_status", "wcvp_gbif_id", "wcvp_canonical_name", "wcvp_status",
                  "match_type", "match_confidence", "annotation_count", "notes"]
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
        "by_gbif_status": dict(Counter(r["gbif_backbone_status"] for r in rows)),
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
