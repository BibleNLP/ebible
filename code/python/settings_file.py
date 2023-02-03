import codecs
from datetime import datetime
import re
from glob import iglob
from os import listdir
from pathlib import Path
from typing import List, Tuple

# Should not need to duplicate this function in ebible.py and here.
def log_and_print(file, s, type="Info") -> None:

    with open(file, "a") as log:
        log.write(
            f"{type.upper()}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {s}\n"
        )
    print(s)


def get_extracted_projects(dir_extracted):

    extracted = []
    for line in listdir(dir_extracted):
        m = re.search(r".+-(.+).txt$", line)
        if m:
            extracted.append(m.group(1))

    return extracted


def get_books_type(files):

    for book in files:
        m = re.search(r".*GEN|JON.*", book)
        if m:
            return "OT+NT"
    return "NT"


def conclude_versification_from_OT(dan_3, dan_5, dan_13):
    if dan_3 == 30:
        versification = "4"  # English
    elif dan_3 == 33 and dan_5 == 30:
        versification = "1"  # Original
    elif dan_3 == 33 and dan_5 == 31:
        versification = "5"  # Russian Protestant
    elif dan_3 == 97:
        versification = "2"  # Septuagint
    elif dan_3 == 100:
        if dan_13 == 65:
            versification = "3"  # Vulgate
        else:
            versification = "6"  # Russian Orthodox
    else:
        versification = ""

    return versification


def conclude_versification_from_NT(jhn_6, act_19, rom_16):
    if jhn_6 == 72:
        versification = "3"  # Vulgate
    elif act_19 == 41:
        versification = "4"  # English
    elif rom_16 == 24:
        versification = "6"  # Russian Orthodox (same as Russian Protestant)
    elif jhn_6 == 71 and act_19 == 40:
        versification = "1"  # Original (Same as Septuagint)
    else:
        versification = ""

    return versification


def get_last_verse(project, book, chapter):

    ch = str(chapter)

    for book_file in iglob(f"{project}/*{book}*"):
        last_verse = "0"
        try:
            f = codecs.open(book_file, "r", encoding="utf-8", errors="ignore")
        except Exception as e:
            log_and_print(logfile, f"Could not open {book_file}, reason:  {e}")
            continue
        try:
            in_chapter = False
            for line in f:
                m = re.search(r"\\c ? ?([0-9]+).*", line)
                if m:
                    if m.group(1) == ch:
                        in_chapter = True
                    else:
                        in_chapter = False

                m = re.search(r"\\v ? ?([0-9]+).*", line)
                if m:
                    if in_chapter:
                        last_verse = m.group(1)
        except Exception as e:
            log_and_print(
                logfile, f"Something went wrong in reading {book_file}, reason:  {e}"
            )
            return None
        try:
            return int(last_verse)
        except Exception as e:
            print(
                f"Could not convert {last_verse} into an integer in {book_file}, reason:  {e}"
            )
            return None


def get_checkpoints_OT(project):
    dan_3 = get_last_verse(project, "DAN", 3)
    dan_5 = get_last_verse(project, "DAN", 5)
    dan_13 = get_last_verse(project, "DAN", 13)

    return dan_3, dan_5, dan_13


def get_checkpoints_NT(project):
    jhn_6 = get_last_verse(project, "JHN", 6)
    act_19 = get_last_verse(project, "ACT", 19)
    rom_16 = get_last_verse(project, "ROM", 16)

    return jhn_6, act_19, rom_16


def get_versification(project):
    versification = ""
    books = get_books_type(listdir(project))

    if books == "OT+NT":
        dan_3, dan_5, dan_13 = get_checkpoints_OT(project)
        versification = conclude_versification_from_OT(dan_3, dan_5, dan_13)

    if not versification:
        jhn_6, act_19, rom_16 = get_checkpoints_NT(project)
        versification = conclude_versification_from_NT(jhn_6, act_19, rom_16)

    if versification != "":
        return versification
    else:
        return "4"  # English


def add_settings_file(project_folder, language_code):
    versification = get_versification(project_folder)
    setting_file_stub = f"""<ScriptureText>
    <Versification>{versification}</Versification>
    <LanguageIsoCode>{language_code}:::</LanguageIsoCode>
    <Naming BookNameForm="41-MAT" PostPart="{project_folder.name}.usfm" PrePart="" />
</ScriptureText>"""

    settings_file = project_folder / "Settings.xml"
    with open(settings_file, "w") as settings:
        settings.write(setting_file_stub)


def write_settings_file(project_folder: Path) -> Path:

    # Now add a Settings.xml file to a project folder.
    if project_folder.is_dir():
        language_code = str(project_folder.name)[:3]
        settings_file = project_folder / "Settings.xml"
        
        if settings_file.is_file():
            return None
        else:
            # print(f"Adding Settings.xml to {project_folder}")
            versification = get_versification(project_folder)
            setting_file_text = f"""<ScriptureText>
                                <Versification>{versification}</Versification>
                                <LanguageIsoCode>{language_code}:::</LanguageIsoCode>
                                <Naming BookNameForm="41-MAT" PostPart="{project_folder.name}.usfm" PrePart="" />
                                </ScriptureText>"""

    
            with open(settings_file, "w") as settings:
                settings.write(setting_file_text)
            return settings_file


def write_settings_files_orig(base_folder: Path) -> Tuple[int, int]:

    # Add a Settings.xml file to each project folder in a base folder.
    project_folders = [project_folder for project_folder in base_folder.glob("*") if project_folder.is_dir()]
    count_new_settings_files = sum(write_settings_file(project_folder) for project_folder in project_folders)
    count_existing_settings_files = len(project_folders) - count_new_settings_files    

    return count_new_settings_files, count_existing_settings_files
