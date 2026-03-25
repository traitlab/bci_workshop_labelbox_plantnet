"""
Phase 0d — Derive species list for Project A ontology from crosswalk CSV.

Steps:
  1. Load gbif_crosswalk.csv (all annotated taxa with annotation counts)
  2. Merge duplicate canonical names (two backbone IDs resolving to same name)
  3. For every species-level taxon, ensure its genus is also in the list
     (genus-level entries are added with annotation_count=0 and wcvp data looked up
      from the species' family/genus context already in the crosswalk or GBIF)
  4. For every genus-level taxon, ensure its family is also in the list
  5. Add an empty/no-classification option
  6. Check ontology size: unique options x 4 annotation types <= 4,000
  7. Output: output/02_species_list/bci_species_list.csv

Output columns:
  rank | gbif_backbone_id | gbif_canonical_name | original_name | gbif_backbone_status |
  wcvp_gbif_id | wcvp_canonical_name | wcvp_status | annotation_count | notes

Usage:
  python scripts/03_species_list/03_build_species_list.py
"""

import csv
import json
import time
from collections import defaultdict
from pathlib import Path

import requests
import yaml

GBIF_API = "https://api.gbif.org/v1/species"
WCVP_DATASET_KEY = "f382f0ce-323a-4091-bb9f-add557f3a9a2"
REQUEST_DELAY = 0.5
CACHE_FILE = Path("output/01_crosswalk/gbif_api_cache.json")

# Families synonymised under modern WCVP taxonomy — map GBIF backbone name to WCVP name
FAMILY_REMAPS = {
    "Cordiaceae": "Boraginaceae",  # Cordia moved into Boraginaceae under APG IV / WCVP
}

_cache: dict = {}


def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.load(open(CACHE_FILE))
    return {}


def save_cache(cache: dict):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(cache, open(CACHE_FILE, "w"), indent=2)


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def gbif_lookup(gbif_id: str) -> dict:
    key = f"lookup:{gbif_id}"
    if key in _cache:
        return _cache[key]
    url = f"{GBIF_API}/{gbif_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = data
    save_cache(_cache)
    time.sleep(REQUEST_DELAY)
    return data


def match_wcvp(canonical_name: str) -> dict:
    """Search WCVP dataset for a canonical name. Returns wcvp_gbif_id, wcvp_canonical_name, wcvp_status."""
    if not canonical_name:
        return {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": ""}
    key = f"wcvp:{canonical_name}"
    if key in _cache:
        results = _cache[key]
    else:
        params = {"q": canonical_name, "datasetKey": WCVP_DATASET_KEY, "limit": 5}
        resp = requests.get(f"{GBIF_API}/search", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        _cache[key] = results
        save_cache(_cache)
        time.sleep(REQUEST_DELAY)
    for r in results:
        if r.get("canonicalName", "").lower() == canonical_name.lower():
            return {
                "wcvp_gbif_id": str(r["key"]),
                "wcvp_canonical_name": r.get("canonicalName", ""),
                "wcvp_status": r.get("taxonomicStatus", ""),
            }
    # Fuzzy: first result with same genus
    if results:
        r = results[0]
        if r.get("canonicalName", "").split()[:1] == canonical_name.split()[:1]:
            return {
                "wcvp_gbif_id": str(r["key"]),
                "wcvp_canonical_name": r.get("canonicalName", ""),
                "wcvp_status": r.get("taxonomicStatus", ""),
            }
    return {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": ""}


def find_parent_family(genus_backbone_id: str, genus_canonical_name: str) -> dict | None:
    """
    Look up the family for a genus using its GBIF backbone ID (follows familyKey directly).
    Falls back to name search restricted to kingdom=Plantae if no backbone ID available.
    Returns {gbif_backbone_id, gbif_canonical_name, gbif_backbone_status, wcvp_gbif_id, wcvp_canonical_name, wcvp_status}
    or None if not found.
    """
    genus_data = None

    if genus_backbone_id:
        genus_data = gbif_lookup(genus_backbone_id)
    else:
        # Search backbone restricted to Plantae to avoid homonym hits
        search_key = f"backbone_genus_plantae:{genus_canonical_name}"
        if search_key in _cache:
            results = _cache[search_key]
        else:
            params = {"q": genus_canonical_name, "rank": "GENUS", "kingdom": "Plantae", "limit": 5}
            resp = requests.get(f"{GBIF_API}/search", params=params, timeout=10)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            _cache[search_key] = results
            save_cache(_cache)
            time.sleep(REQUEST_DELAY)
        plant_kingdoms = {"Plantae", "Viridiplantae"}
        for r in results:
            if (r.get("canonicalName", "").lower() == genus_canonical_name.lower()
                    and r.get("kingdom") in plant_kingdoms):
                genus_data = r
                break

    if not genus_data:
        return None

    # Use familyKey directly if present (most reliable)
    family_key = genus_data.get("familyKey")
    if family_key:
        family_data = gbif_lookup(str(family_key))
    else:
        parent_key = genus_data.get("parentKey")
        if not parent_key:
            return None
        family_data = gbif_lookup(str(parent_key))
        if family_data.get("rank") != "FAMILY":
            parent_key2 = family_data.get("parentKey")
            if parent_key2:
                family_data = gbif_lookup(str(parent_key2))
            if family_data.get("rank") != "FAMILY":
                return None

    family_name = family_data.get("canonicalName", "")
    wcvp_name = FAMILY_REMAPS.get(family_name, family_name)
    wcvp = match_wcvp(wcvp_name)
    if wcvp_name != family_name and wcvp["wcvp_gbif_id"]:
        wcvp["wcvp_canonical_name"] = wcvp.get("wcvp_canonical_name") or wcvp_name
    return {
        "gbif_backbone_id": str(family_data.get("key", "")),
        "gbif_canonical_name": family_name,
        "gbif_backbone_status": family_data.get("taxonomicStatus", ""),
        **wcvp,
    }


def main():
    global _cache
    _cache = load_cache()
    print(f"  Loaded {len(_cache)} cached API responses")

    config = load_config()
    output_dir = Path(config["folders"]["species_list"])
    output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1 — Load crosswalk, merge duplicate canonical names
    # ------------------------------------------------------------------
    print("\nStep 1 — Loading crosswalk ...")
    crosswalk_path = Path(config["folders"]["crosswalk"]) / "gbif_crosswalk.csv"
    raw_rows = list(csv.DictReader(open(crosswalk_path)))

    # Group by (rank, gbif_canonical_name) — merge duplicates
    # Keep the row with highest annotation_count as primary; sum counts
    groups: dict[tuple, list] = defaultdict(list)
    for r in raw_rows:
        groups[(r["rank"], r["gbif_canonical_name"])].append(r)

    merged: list[dict] = []
    for (rank, name), group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
        else:
            primary = max(group, key=lambda r: int(r["annotation_count"]))
            total_count = sum(int(r["annotation_count"]) for r in group)
            merged_ids = ", ".join(r["gbif_backbone_id"] for r in group)
            entry = dict(primary)
            entry["annotation_count"] = str(total_count)
            entry["notes"] = f"merged backbone IDs: {merged_ids}" + (f"; {primary['notes']}" if primary["notes"] else "")
            merged.append(entry)
            print(f"  Merged duplicate: {rank} '{name}' ({merged_ids}) -> count={total_count}")

    print(f"  {len(raw_rows)} crosswalk rows -> {len(merged)} after merging duplicates")

    # Build lookup by canonical name per rank
    by_name: dict[tuple, dict] = {(r["rank"], r["gbif_canonical_name"]): r for r in merged}

    # ------------------------------------------------------------------
    # Step 2 — Ensure genera for all species are present
    # ------------------------------------------------------------------
    print("\nStep 2 — Adding missing genera ...")
    added_genera = 0
    for r in list(merged):
        if r["rank"] != "SPECIES":
            continue
        genus = r["gbif_canonical_name"].split()[0]
        if ("GENUS", genus) not in by_name:
            print(f"  Adding genus: {genus} (from species {r['gbif_canonical_name']})")
            wcvp = match_wcvp(genus)
            entry = {
                "rank": "GENUS",
                "gbif_backbone_id": "",
                "original_name": genus,
                "original_rank": "",
                "gbif_canonical_name": genus,
                "gbif_backbone_status": "",
                "wcvp_gbif_id": wcvp["wcvp_gbif_id"],
                "wcvp_canonical_name": wcvp["wcvp_canonical_name"],
                "wcvp_status": wcvp["wcvp_status"],
                "match_type": "EXACT" if wcvp["wcvp_gbif_id"] else "NONE",
                "match_confidence": "100" if wcvp["wcvp_gbif_id"] else "",
                "annotation_count": "0",
                "notes": "added: parent genus of annotated species",
            }
            merged.append(entry)
            by_name[("GENUS", genus)] = entry
            added_genera += 1
    print(f"  Added {added_genera} genera")

    # ------------------------------------------------------------------
    # Step 3 — Ensure families for all genera are present
    # ------------------------------------------------------------------
    print("\nStep 3 — Adding missing families ...")
    added_families = 0
    # Rebuild by_name to include newly added genera
    by_name = {(r["rank"], r["gbif_canonical_name"]): r for r in merged}

    # Collect all families already present
    present_families = {r["gbif_canonical_name"] for r in merged if r["rank"] == "FAMILY"}

    new_families: dict[str, dict] = {}  # family_name -> entry
    for r in [r for r in merged if r["rank"] == "GENUS"]:
        genus = r["gbif_canonical_name"]
        backbone_id = r.get("gbif_backbone_id", "")
        family_info = find_parent_family(backbone_id, genus)
        if family_info and family_info["gbif_canonical_name"] not in present_families:
            fname = family_info["gbif_canonical_name"]
            if fname not in new_families:
                new_families[fname] = family_info
                print(f"  Adding family: {fname} (from genus {genus})")

    for fname, info in new_families.items():
        entry = {
            "rank": "FAMILY",
            "gbif_backbone_id": info["gbif_backbone_id"],
            "original_name": fname,
            "original_rank": "",
            "gbif_canonical_name": fname,
            "gbif_backbone_status": info["gbif_backbone_status"],
            "wcvp_gbif_id": info["wcvp_gbif_id"],
            "wcvp_canonical_name": info["wcvp_canonical_name"],
            "wcvp_status": info["wcvp_status"],
            "match_type": "EXACT" if info["wcvp_gbif_id"] else "NONE",
            "match_confidence": "100" if info["wcvp_gbif_id"] else "",
            "annotation_count": "0",
            "notes": "added: parent family of annotated genus",
        }
        merged.append(entry)
        added_families += 1
    print(f"  Added {added_families} families")

    # ------------------------------------------------------------------
    # Step 4 — Check ontology size
    # ------------------------------------------------------------------
    # +1 for the empty/no-classification option
    n_options = len(merged) + 1
    n_annotation_types = 4
    ontology_size = n_options * n_annotation_types
    print(f"\nStep 4 — Ontology size check:")
    print(f"  {n_options} options (including empty) x {n_annotation_types} annotation types = {ontology_size}")
    if ontology_size > 4000:
        print(f"  WARNING: ontology size {ontology_size} exceeds 4,000 limit!")
    else:
        print(f"  OK: within 4,000 limit")

    # ------------------------------------------------------------------
    # Step 5 — Write output CSV
    # ------------------------------------------------------------------
    # Sort: families first, then genera, then species; within each rank by annotation_count desc
    rank_order = {"FAMILY": 0, "GENUS": 1, "SPECIES": 2}
    merged.sort(key=lambda r: (rank_order.get(r["rank"], 9), -int(r["annotation_count"])))

    fieldnames = [
        "rank", "gbif_backbone_id", "gbif_canonical_name", "original_name",
        "gbif_backbone_status", "wcvp_gbif_id", "wcvp_canonical_name", "wcvp_status",
        "annotation_count", "notes",
    ]
    csv_file = output_dir / "bci_species_list.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    print(f"\nSpecies list written to {csv_file}")
    print(f"  Families: {sum(1 for r in merged if r['rank'] == 'FAMILY')}")
    print(f"  Genera:   {sum(1 for r in merged if r['rank'] == 'GENUS')}")
    print(f"  Species:  {sum(1 for r in merged if r['rank'] == 'SPECIES')}")
    print(f"  Total taxon options: {len(merged)}")
    print(f"  With empty option: {n_options}")
    wcvp_matched = sum(1 for r in merged if r["wcvp_gbif_id"])
    print(f"  WCVP matched: {wcvp_matched}/{len(merged)}")

    # Summary JSON
    summary = {
        "total_options_incl_empty": n_options,
        "ontology_size_4x": ontology_size,
        "ontology_limit": 4000,
        "within_limit": ontology_size <= 4000,
        "families": sum(1 for r in merged if r["rank"] == "FAMILY"),
        "genera": sum(1 for r in merged if r["rank"] == "GENUS"),
        "species": sum(1 for r in merged if r["rank"] == "SPECIES"),
        "added_genera": added_genera,
        "added_families": added_families,
        "wcvp_matched": wcvp_matched,
        "wcvp_unmatched": len(merged) - wcvp_matched,
    }
    summary_file = output_dir / "species_list_summary.json"
    json.dump(summary, open(summary_file, "w"), indent=2)
    print(f"Summary written to {summary_file}")


if __name__ == "__main__":
    main()
