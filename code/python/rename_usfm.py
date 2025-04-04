import os
import regex
from pathlib import Path


def rename_usfm(project_dir: Path) -> None:
    """
    Renames the .usfm files within a project to a standardized format by removing the leading 3 characters in some cases.

    The .usfm files in the zips have numerical prefixes, e.g.
      01-INTcmn2006.usfm
      02-GENcmn2006.usfm
      03-EXOcmn2006.usfm
      04-LEVcmn2006.usfm
      05-NUMcmn2006.usfm
      06-DEUcmn2006.usfm
      ...
    There is no consistency for which numbers are assigned to which books across the eBible.org projects.
    So it is simpler to remove the leading 3 characters, then do:
        // Settings.xml
        <Naming BookNameForm="MAT"

    Currently we only remove the prefixes.
    We could potentially also change the suffix of the filenames to align with the renamed project format,
    Project: eng-web-c    -> web_c
    File:    MATeng-web-c -> MATweb_c
    That might be less confusing than the mix we have now.
    See also settings_file.py `write_settings_file` and ebible.py `create_project_name`.
    """
    usfm_paths = list(project_dir.glob("*.usfm"))
    for old_usfm_path in usfm_paths:
        name = old_usfm_path.name
        if regex.match("^\d\d-", name):
            new_name: str = old_usfm_path.name[3:]
            new_usfm_path: Path = old_usfm_path.parent / new_name
            os.rename(old_usfm_path, new_usfm_path)
