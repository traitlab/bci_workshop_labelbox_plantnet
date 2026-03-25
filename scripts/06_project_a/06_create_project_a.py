"""
Phase 0f — Create Project A ontology and project in Labelbox.

Creates a single unified ontology with 4 annotation types sharing the same
taxon option list (from bci_species_list.csv):
  - [Global] Radio: "Dominant taxon"
  - [Global] Checklist: "Taxa present"
  - [Tool] BBOX: "Plant box" with nested Radio "Taxon"
  - [Tool] RASTER_SEGMENTATION: "Plant mask" with nested Radio "Taxon"

Creates a project, connects the ontology, and sends data rows from the
combined dataset.

Usage:
  python scripts/06_project_a/06_create_project_a.py
"""

import csv
import os
import sys
from pathlib import Path

import labelbox as lb
from dotenv import load_dotenv
import yaml


def load_config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def build_taxon_options(species_list_path: Path) -> list[lb.Option]:
    """Load taxon list CSV and build Labelbox Option objects, deduplicated by wcvp_gbif_id."""
    seen_ids = set()
    options = []
    with open(species_list_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            wcvp_id = row["wcvp_gbif_id"].strip()
            name = row["wcvp_canonical_name"].strip()
            if not wcvp_id or not name:
                continue
            if wcvp_id in seen_ids:
                continue
            seen_ids.add(wcvp_id)
            options.append(lb.Option(value=wcvp_id, label=name))
    options.sort(key=lambda o: o.label)
    return options


def main():
    load_dotenv()
    config = load_config()

    project_name = config["labelbox"]["project_a_name"]
    ontology_name = config["labelbox"]["project_a_ontology_name"]
    dataset_name = config["labelbox"]["combined_dataset_name"]
    species_list_path = Path(config["folders"]["species_list"]) / "bci_species_list.csv"

    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])

    # ------------------------------------------------------------------
    # Step 1 — Build taxon options
    # ------------------------------------------------------------------
    print("Step 1 - Loading taxon list ...")
    options = build_taxon_options(species_list_path)
    print(f"  {len(options)} taxon options loaded")

    # ------------------------------------------------------------------
    # Step 2 — Build ontology
    # ------------------------------------------------------------------
    print("\nStep 2 - Building ontology ...")

    # Check if ontology already exists
    existing_ontology = None
    for ont in client.get_ontologies(ontology_name):
        if ont.name == ontology_name:
            existing_ontology = ont
            break

    if existing_ontology:
        print(f"  Ontology already exists: {existing_ontology.uid}")
        ontology = existing_ontology
    else:
        ontology_builder = lb.OntologyBuilder(
            classifications=[
                lb.Classification(
                    class_type=lb.Classification.Type.RADIO,
                    name="Dominant taxon",
                    options=list(options),
                ),
                lb.Classification(
                    class_type=lb.Classification.Type.CHECKLIST,
                    name="Taxa present",
                    options=list(options),
                ),
            ],
            tools=[
                lb.Tool(
                    tool=lb.Tool.Type.BBOX,
                    name="Plant box",
                    classifications=[
                        lb.Classification(
                            class_type=lb.Classification.Type.RADIO,
                            name="Taxon",
                            options=list(options),
                        ),
                    ],
                ),
                lb.Tool(
                    tool=lb.Tool.Type.RASTER_SEGMENTATION,
                    name="Plant mask",
                    classifications=[
                        lb.Classification(
                            class_type=lb.Classification.Type.RADIO,
                            name="Taxon",
                            options=list(options),
                        ),
                    ],
                ),
            ],
        )

        ontology = client.create_ontology(
            name=ontology_name,
            normalized=ontology_builder.asdict(),
            media_type=lb.MediaType.Image,
        )
        print(f"  Created ontology: {ontology.uid}")

    # ------------------------------------------------------------------
    # Step 3 — Create project
    # ------------------------------------------------------------------
    print("\nStep 3 - Creating project ...")

    existing_project = None
    for proj in client.get_projects():
        if proj.name == project_name:
            existing_project = proj
            break

    if existing_project:
        print(f"  Project already exists: {existing_project.uid}")
        project = existing_project
    else:
        project = client.create_project(
            name=project_name,
            media_type=lb.MediaType.Image,
        )
        project.connect_ontology(ontology)
        print(f"  Created project: {project.uid}")
        print(f"  Connected ontology: {ontology.uid}")

    # ------------------------------------------------------------------
    # Step 4 — Send data rows from combined dataset
    # ------------------------------------------------------------------
    print("\nStep 4 - Sending data rows to project ...")

    dataset = None
    for d in client.get_datasets():
        if d.name == dataset_name:
            dataset = d
            break

    if dataset is None:
        print(f"  ERROR: Dataset '{dataset_name}' not found")
        sys.exit(1)

    # Check if batches already exist
    existing_batches = list(project.batches())
    if existing_batches:
        total_rows = sum(b.size for b in existing_batches)
        print(f"  Project already has {len(existing_batches)} batch(es) with {total_rows} rows")
    else:
        task = project.create_batches_from_dataset(
            name_prefix="bci_workshop",
            dataset_id=dataset.uid,
        )
        task.wait_till_done()
        print(f"  Sent all data rows from '{dataset_name}' to project")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\nDone.")
    print(f"  Ontology: {ontology.name} ({ontology.uid})")
    print(f"  Project:  {project.name} ({project.uid})")
    print(f"  Dataset:  {dataset_name}")
    print(f"  Taxon options: {len(options)}")


if __name__ == "__main__":
    main()
