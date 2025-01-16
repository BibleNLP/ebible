import os
from pathlib import Path

def rename_usfm(projects_dir:Path):
    old_name_new_name: list[tuple] = []
    for dir in projects_dir.glob("*/*.usfm"):
        name = dir.name
        if not name[:2].isdigit():
            continue
        old_name = dir 
        new_name = dir.parent / name[3:]
        old_name_new_name.append((old_name, new_name))
    
    for old_name, new_name in old_name_new_name:
        os.rename(old_name, new_name)