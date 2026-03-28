"""
Phase 1-multi step a — Update Project A ontology for multi-species predictions.

Changes to the ontology:
  1. Add nested Text "Cover (%)" inside every option of global Radio "Taxon"
  2. Add nested Text "Cover (%)" inside every option of global Checklist "Taxa"
  3. Remove global Checklist "Organs" (organs now live only inside BBOX "Plant box")

Strategy:
  - Fetch current ontology normalized schema
  - Modify the schema in-memory (add nested text, remove global Organs)
  - Create a NEW ontology with the updated schema
  - Disconnect old ontology from Project A, connect the new one

Usage:
  python scripts/14_multi_predictions/14a_update_ontology.py --dry-run   # preview changes
  python scripts/14_multi_predictions/14a_update_ontology.py             # apply changes
"""

import argparse
import copy
import json
import os
import sys
from pathlib import Path

import labelbox as lb
import yaml
from dotenv import load_dotenv

PROJECT_A_NAME = "BCI Workshop - All Label Types"
PROJECT_A_UID = "cmn6iicta01w3070sggxmf00q"

COVER_TEXT = {
    "type": "text",
    "instructions": "Cover (%)",
    "name": "Cover (%)",
    "required": False,
    "options": [],
    "schemaNodeId": None,
    "featureSchemaId": None,
}


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def add_cover_to_options(options: list) -> int:
    """Add nested Text 'Cover (%)' to each option that doesn't already have it.
    Returns number of options modified."""
    count = 0
    for opt in options:
        nested = opt.get("options", [])
        has_cover = any(
            n.get("name") == "Cover (%)" and n.get("type") == "text"
            for n in nested
        )
        if not has_cover:
            opt["options"] = nested + [copy.deepcopy(COVER_TEXT)]
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Update Project A ontology: add Cover (%), remove global Organs."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without creating new ontology")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    api_key = os.environ.get("LABELBOX_API_KEY")
    if not api_key:
        sys.exit("ERROR: LABELBOX_API_KEY not found in .env")

    client = lb.Client(api_key=api_key)

    # Step 1: Find project and current ontology
    print("Step 1 — Finding Project A and its ontology ...")
    project = next((p for p in client.get_projects() if p.name == PROJECT_A_NAME), None)
    if project is None:
        sys.exit(f"ERROR: Project '{PROJECT_A_NAME}' not found.")
    if project.uid != PROJECT_A_UID:
        sys.exit(f"ERROR: Project UID mismatch! Expected {PROJECT_A_UID}, got {project.uid}")

    old_ontology = project.ontology()
    print(f"  Project: {project.name} ({project.uid})")
    print(f"  Current ontology: {old_ontology.name} ({old_ontology.uid})")

    # Step 2: Get and modify the normalized schema
    print("\nStep 2 — Modifying ontology schema ...")
    schema = copy.deepcopy(old_ontology.normalized)

    # 2a. Add Cover (%) to global Radio "Taxon" and Checklist "Taxa"
    modified_classifications = []
    removed_organs = False
    kept_classifications = []

    for cls in schema.get("classifications", []):
        name = cls.get("name") or cls.get("instructions", "")
        cls_type = cls.get("type", "")

        # Remove global Organs checklist
        if name == "Organs" and cls_type == "checklist":
            print(f"  Removing global Checklist 'Organs'")
            removed_organs = True
            continue

        # Add Cover (%) to Taxon radio and Taxa checklist
        if name == "Taxon" and cls_type == "radio":
            n = add_cover_to_options(cls.get("options", []))
            print(f"  Added Cover (%) to {n} options of Radio 'Taxon'")
            modified_classifications.append(name)
        elif name == "Taxa" and cls_type == "checklist":
            n = add_cover_to_options(cls.get("options", []))
            print(f"  Added Cover (%) to {n} options of Checklist 'Taxa'")
            modified_classifications.append(name)

        kept_classifications.append(cls)

    schema["classifications"] = kept_classifications

    if not modified_classifications:
        sys.exit("ERROR: Could not find Radio 'Taxon' or Checklist 'Taxa' in ontology.")

    if not removed_organs:
        print("  NOTE: Global Checklist 'Organs' was not found (may have been removed already)")

    # Verify tools still have nested Organs inside BBOX
    for tool in schema.get("tools", []):
        if tool.get("name") == "Plant box":
            nested_names = [c.get("name", "") for c in tool.get("classifications", [])]
            print(f"  BBOX 'Plant box' nested classifications: {nested_names}")

    if args.dry_run:
        # Save preview
        preview_path = Path("output/14_multi_predictions/ontology_preview.json")
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        with open(preview_path, "w") as f:
            json.dump(schema, f, indent=2)
        print(f"\n  DRY RUN: Preview saved to {preview_path}")
        print("  Review and re-run without --dry-run to apply.")
        return

    # Step 3: Create new ontology
    new_ontology_name = old_ontology.name  # same name, Labelbox allows duplicates
    print(f"\nStep 3 — Creating new ontology '{new_ontology_name}' ...")

    # Strip schema node IDs and feature schema IDs so Labelbox creates fresh ones
    def strip_ids(obj):
        if isinstance(obj, dict):
            obj.pop("schemaNodeId", None)
            obj.pop("featureSchemaId", None)
            for v in obj.values():
                strip_ids(v)
        elif isinstance(obj, list):
            for item in obj:
                strip_ids(item)

    strip_ids(schema)

    new_ontology = client.create_ontology(
        name=new_ontology_name,
        normalized=schema,
        media_type=lb.MediaType.Image,
    )
    print(f"  New ontology created: {new_ontology.uid}")

    # Step 4: Reconnect project to new ontology
    print(f"\nStep 4 — Connecting project to new ontology ...")
    project.connect_ontology(new_ontology)
    print(f"  Project now uses ontology: {new_ontology.uid}")

    # Step 5: Verify
    print(f"\nStep 5 — Verifying ...")
    refreshed = project.ontology()
    print(f"  Project ontology ID: {refreshed.uid}")

    cls_names = [c.get("name", c.get("instructions", "")) for c in refreshed.normalized.get("classifications", [])]
    tool_names = [t.get("name") for t in refreshed.normalized.get("tools", [])]
    print(f"  Global classifications: {cls_names}")
    print(f"  Tools: {tool_names}")

    # Check Cover (%) nesting
    for cls in refreshed.normalized.get("classifications", []):
        name = cls.get("name") or cls.get("instructions", "")
        if name in ("Taxon", "Taxa"):
            first_opt = cls.get("options", [{}])[0] if cls.get("options") else {}
            nested = first_opt.get("options", [])
            nested_names = [n.get("name", "") for n in nested]
            print(f"  {name} first option nested: {nested_names}")

    print(f"\n{'=' * 55}")
    print("ONTOLOGY UPDATE COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Old ontology: {old_ontology.uid}")
    print(f"  New ontology: {new_ontology.uid}")
    print(f"  Changes: Cover (%) added to Taxon + Taxa, global Organs removed")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
