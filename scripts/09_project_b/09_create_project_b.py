"""
Phase 0i — Create Project B ontology and project in Labelbox.

Source taxon list: ontology cm9fy6wm00xis073obwoa5228
  ("BCI close-up photo segmentation - single list (espanol)")
  1,931 options with GBIF backbone IDs as values.

For each option, resolves the GBIF backbone ID → WCVP GBIF ID using the
same GBIF API logic as Phase 0c (reuses existing cache).

Ontology structure:
  - [Tool] BBOX: "Planta"
    - Nested Radio: "Taxón"      — label = original label string,
                                   value = WCVP GBIF ID (fallback: GBIF backbone ID)
    - Nested Checklist: "Órgano" — Flor (flower) / Fruta (fruit)

Run in two steps:
  Step 1 (default): resolve taxa, print summary, save crosswalk to
    output/09_project_b/project_b_taxon_crosswalk.csv — review before proceeding
  Step 2 (--create): create ontology + project in Labelbox

Usage:
  python scripts/09_project_b/09_create_project_b.py           # resolve + review
  python scripts/09_project_b/09_create_project_b.py --create  # create in Labelbox
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path

import requests
import yaml
import labelbox as lb
from dotenv import load_dotenv

SOURCE_ONTOLOGY_ID = "cm9fy6wm00xis073obwoa5228"
GBIF_API = "https://api.gbif.org/v1/species"
WCVP_DATASET_KEY = "f382f0ce-323a-4091-bb9f-add557f3a9a2"
REQUEST_DELAY = 0.3
CACHE_FILE = Path("output/01_crosswalk/gbif_api_cache.json")
OUTPUT_DIR = Path("output/09_project_b")

FAMILY_REMAPS = {
    "Hippocrateaceae": "Celastraceae",
}

# Families not recognized by WCVP — map directly to their WCVP accepted family GBIF ID
FAMILY_WCVP_OVERRIDES = {
    "Cochlospermaceae": "316786928",   # -> Bixaceae
    "Cordiaceae":       "316786931",   # -> Boraginaceae
    "Coulaceae":        "316787189",   # -> Olacaceae
    "Ehretiaceae":      "316786931",   # -> Boraginaceae
    "Erythropalaceae":  "316787189",   # -> Olacaceae
    "Heliotropiaceae":  "316786931",   # -> Boraginaceae
    "Quiinaceae":       "316787188",   # -> Ochnaceae
    "Stixaceae":        "316786957",   # -> Capparaceae
    "Turneraceae":      "316787205",   # -> Passifloraceae
    "Ximeniaceae":      "316787189",   # -> Olacaceae
}

_cache: dict = {}


def load_cache():
    global _cache
    if CACHE_FILE.exists():
        _cache = json.load(open(CACHE_FILE))
        print(f"  Loaded {len(_cache)} cached API responses")
    else:
        _cache = {}


def save_cache():
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(_cache, open(CACHE_FILE, "w"), indent=2)


def gbif_lookup(gbif_id: str) -> dict:
    key = f"lookup:{gbif_id}"
    if key in _cache:
        return _cache[key]
    resp = requests.get(f"{GBIF_API}/{gbif_id}", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = data
    save_cache()
    time.sleep(REQUEST_DELAY)
    return data


def resolve_taxon(gbif_id: str) -> dict:
    data = gbif_lookup(gbif_id)
    original_rank = data.get("rank", "")
    status = data.get("taxonomicStatus", "")
    notes = []

    if status in ("SYNONYM", "HETEROTYPIC_SYNONYM", "HOMOTYPIC_SYNONYM", "PROPARTE_SYNONYM"):
        accepted_key = data.get("acceptedKey") or data.get("accepted", {}).get("key")
        if accepted_key:
            notes.append(f"synonym resolved to {accepted_key}")
            data = gbif_lookup(str(accepted_key))
            status = data.get("taxonomicStatus", "")

    rank = data.get("rank", "")
    canonical_name = data.get("canonicalName", "")

    if rank == "VARIETY":
        parent_key = data.get("parentKey")
        if parent_key:
            notes.append("rolled up from VARIETY to species")
            parent_data = gbif_lookup(str(parent_key))
            canonical_name = parent_data.get("canonicalName", canonical_name)
            rank = parent_data.get("rank", rank)

    return {
        "canonical_name": canonical_name,
        "rank": rank,
        "original_rank": original_rank,
        "gbif_status": status,
        "notes": "; ".join(notes),
    }


def match_wcvp(canonical_name: str) -> dict:
    if not canonical_name:
        return {"wcvp_gbif_id": "", "match_type": "NONE"}

    search_name = FAMILY_REMAPS.get(canonical_name, canonical_name)
    key = f"wcvp:{canonical_name}"
    if key in _cache:
        results = _cache[key]
    else:
        params = {"q": search_name, "datasetKey": WCVP_DATASET_KEY, "limit": 5}
        resp = requests.get(f"{GBIF_API}/search", params=params, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        _cache[key] = results
        save_cache()
        time.sleep(REQUEST_DELAY)

    def resolve_accepted(r):
        if r.get("taxonomicStatus") == "ACCEPTED":
            return r
        ak = r.get("acceptedKey")
        if not ak:
            return None
        return gbif_lookup(str(ak))

    def wcvp_result(r, match_type):
        return {
            "wcvp_gbif_id": str(r.get("key", r.get("usageKey", ""))),
            "wcvp_canonical_name": r.get("canonicalName", ""),
            "wcvp_status": r.get("taxonomicStatus", ""),
            "match_type": match_type,
        }

    exact = [r for r in results if r.get("canonicalName", "").lower() == search_name.lower()]
    accepted = next((r for r in exact if r.get("taxonomicStatus") == "ACCEPTED"), None)
    if not accepted and exact:
        accepted = resolve_accepted(exact[0])
    if accepted:
        return wcvp_result(accepted, "EXACT")

    if results:
        q_parts = canonical_name.lower().split()
        same_genus = [r for r in results if r.get("canonicalName", "").lower().split()[:1] == q_parts[:1]]
        r = next((r for r in same_genus if r.get("taxonomicStatus") == "ACCEPTED"), same_genus[0] if same_genus else None)
        if r:
            if r.get("taxonomicStatus") != "ACCEPTED":
                r = resolve_accepted(r) or r
            return wcvp_result(r, "FUZZY")

    return {"wcvp_gbif_id": "", "wcvp_canonical_name": "", "wcvp_status": "", "match_type": "NONE"}


def fetch_source_options(client: lb.Client) -> list[dict]:
    ontology = client.get_ontology(SOURCE_ONTOLOGY_ID)
    schema = ontology.normalized
    options = []
    for tool in schema["tools"]:
        for cls in tool.get("classifications", []):
            options.extend(cls.get("options", []))
    print(f"  {len(options)} options from source ontology '{ontology.name}'")
    return options


def resolve_all(options: list[dict]) -> list[dict]:
    rows = []
    unmatched = []
    for i, opt in enumerate(options, 1):
        gbif_id = str(opt["value"])
        label = opt["label"]
        if i % 100 == 0 or i <= 5:
            print(f"  [{i}/{len(options)}] {label} ...")
        try:
            resolved = resolve_taxon(gbif_id)
            wcvp = match_wcvp(resolved["canonical_name"])
        except Exception as e:
            resolved = {"canonical_name": "", "rank": "", "original_rank": "", "gbif_status": "ERROR", "notes": str(e)}
            wcvp = {"wcvp_gbif_id": "", "match_type": "ERROR"}

        # Apply direct WCVP overrides for families not in WCVP under their original name
        if not wcvp["wcvp_gbif_id"] and resolved["canonical_name"] in FAMILY_WCVP_OVERRIDES:
            override_id = FAMILY_WCVP_OVERRIDES[resolved["canonical_name"]]
            override_rec = gbif_lookup(override_id)
            wcvp = {
                "wcvp_gbif_id": override_id,
                "wcvp_canonical_name": override_rec.get("canonicalName", ""),
                "wcvp_status": override_rec.get("taxonomicStatus", ""),
                "match_type": "OVERRIDE",
            }

        if not wcvp["wcvp_gbif_id"]:
            unmatched.append(f"  {label} (GBIF {gbif_id}) — {resolved['canonical_name']} [{wcvp['match_type']}]")

        rows.append({
            "label": label,
            "gbif_backbone_id": gbif_id,
            "original_rank": resolved["original_rank"],
            "gbif_canonical_name": resolved["canonical_name"],
            "rank": resolved["rank"],
            "gbif_backbone_status": resolved["gbif_status"],
            "wcvp_gbif_id": wcvp["wcvp_gbif_id"],
            "wcvp_canonical_name": wcvp.get("wcvp_canonical_name", ""),
            "wcvp_status": wcvp.get("wcvp_status", ""),
            "match_type": wcvp["match_type"],
            "notes": resolved["notes"],
        })

    if unmatched:
        print(f"\n  WARNING — {len(unmatched)} unmatched taxa (will use GBIF backbone ID as fallback):")
        for line in unmatched:
            print(line)
    else:
        print(f"\n  All {len(rows)} taxa matched to WCVP.")

    return rows


def build_ontology(client: lb.Client, rows: list[dict], ontology_name: str) -> lb.Ontology:
    organ_options = [
        lb.Option(value="flower", label="Flor"),
        lb.Option(value="fruit", label="Fruta"),
    ]

    # Sort alphabetically by label, then deduplicate on wcvp_gbif_id (fallback to gbif_backbone_id)
    seen_values = set()
    taxon_options = []
    for row in sorted(rows, key=lambda r: r["label"]):
        value = row["wcvp_gbif_id"] or row["gbif_backbone_id"]
        if value in seen_values:
            continue
        seen_values.add(value)
        taxon_options.append(lb.Option(value=value, label=row["label"]))

    ontology_builder = lb.OntologyBuilder(
        tools=[
            lb.Tool(
                tool=lb.Tool.Type.BBOX,
                name="Planta",
                classifications=[
                    lb.Classification(
                        class_type=lb.Classification.Type.RADIO,
                        name="Taxón",
                        options=taxon_options,
                    ),
                    lb.Classification(
                        class_type=lb.Classification.Type.CHECKLIST,
                        name="Órgano",
                        options=organ_options,
                    ),
                ],
            ),
        ],
    )

    print(f"  Creating ontology '{ontology_name}' with {len(taxon_options)} taxon options ...")
    return client.create_ontology(
        name=ontology_name,
        normalized=ontology_builder.asdict(),
        media_type=lb.MediaType.Image,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--create", action="store_true", help="Create ontology + project in Labelbox")
    args = parser.parse_args()

    load_dotenv()
    config = yaml.safe_load(open("config.yaml"))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    api_key = os.environ["LABELBOX_API_KEY"]
    client = lb.Client(api_key=api_key)

    print("Step 1 — Loading cache and fetching source ontology options ...")
    load_cache()
    options = fetch_source_options(client)

    print(f"\nStep 2 — Resolving {len(options)} taxa via GBIF API (using cache where possible) ...")
    rows = resolve_all(options)

    # Save crosswalk CSV
    csv_path = OUTPUT_DIR / "project_b_taxon_crosswalk.csv"
    fieldnames = ["label", "gbif_backbone_id", "original_rank", "gbif_canonical_name", "rank", "gbif_backbone_status", "wcvp_gbif_id", "wcvp_canonical_name", "wcvp_status", "match_type", "notes"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nCrosswalk saved to {csv_path}")

    matched = sum(1 for r in rows if r["wcvp_gbif_id"])
    print(f"Summary: {matched}/{len(rows)} matched to WCVP")
    by_match = {}
    for r in rows:
        by_match[r["match_type"]] = by_match.get(r["match_type"], 0) + 1
    print(f"  by match_type: {by_match}")

    if not args.create:
        print("\nReview the crosswalk CSV, then re-run with --create to build the ontology and project.")
        return

    ontology_name = config["labelbox"].get("project_b_ontology_name", "BCI Workshop - Botanist Labelling")
    project_name = config["labelbox"].get("project_b_name", "BCI Workshop - Botanist Labelling")

    print(f"\nStep 3 — Creating ontology '{ontology_name}' ...")
    ontology = build_ontology(client, rows, ontology_name)
    print(f"  Ontology created: {ontology.uid}")

    print(f"\nStep 4 — Creating project '{project_name}' ...")
    project = client.create_project(
        name=project_name,
        media_type=lb.MediaType.Image,
    )
    project.connect_ontology(ontology)
    print(f"  Project created: {project.uid}")
    print("\nDone. Review ontology in Labelbox before sending data rows.")


if __name__ == "__main__":
    main()
