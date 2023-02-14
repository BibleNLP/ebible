"""
Script to pull out and collect the `copr.html` files from the extracted directories as a poor-man's way of showing the licensing terms of the files used.
"""
from pathlib import Path
from shutil import copy

PROJECTS_DIR = "/mnt/data/share/ebible/fresh_pull/projects"
DESTINATION_DIR = "/mnt/data/share/ebible/metadata/licences"

def get_licences_from_extract():
    """Put all the copr.html files from the extract into 
       `DESTINATION_DIR`."""
    # Initialize Paths
    projects_dir = Path(PROJECTS_DIR)
    destination_dir = Path(DESTINATION_DIR)

    # Iterate project_dir for collecting `copr.html` Paths
    copr_paths = [project / "copr.htm" for project in projects_dir.iterdir()]

    print("Copying over copr.htm files...")
    print(f"Source directory: {projects_dir}")
    print(f"Target directory: {destination_dir}")

    # Copy files over
    for copr_file in copr_paths:
        copy(copr_file, destination_dir / f"{copr_file.parent.name[:3]}-{copr_file.parent.name}-copr.htm")

    print("Finished copying successfully.")


if __name__ == "__main__":
    get_licences_from_extract()
