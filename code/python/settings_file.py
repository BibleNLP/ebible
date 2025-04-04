import codecs
import re
import shutil
from datetime import datetime
from glob import iglob
from os import listdir
from pathlib import Path
from typing import Tuple

import yaml
from machine.corpora import ParatextTextCorpus, extract_scripture_corpus
from machine.scripture.verse_ref import VerseRef

vrs_to_num: dict[str, int] = {
    "Original": 1,
    "Septuagint": 2,
    "Vulgate": 3,
    "English": 4,
    "Russian Protestant": 5,
    "Russian Orthodox": 6,
}

BOOK_NUM = r"[0-9].\-"

EXCLUDE_ALPHANUMERICS = r"[^\w]"

POST_PART = r"[a-z].+"


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


def get_vrs_diffs() -> dict[str, dict[int, dict[int, list[str]]]]:
    """
    gets the differences in versifications from vrs_diffs.yaml
    return: the versification differences
    """
    with open("vrs_diffs.yaml", "r") as file:
        try:
            vrs_diffs = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)
    return vrs_diffs


def check_vref(
    prev: VerseRef,
    vrs_diffs: dict[str, dict[int, dict[int, list[str]]]],
    versifications: list[str],
    ruled_out: list[str],
) -> tuple[list[str], list[str]]:
    """
    checks a verse reference to try to determine the versification
    param prev: the verse reference to check
    param vrs_diffs: the list of differences in the versifications
    param versifications: the list of possible versifications
    param ruled_out: the list of versifications that have been ruled out
    return: the list of possible versifications and the list of versifications that have been ruled out
    """
    try:
        curr = vrs_diffs[prev.book]["last_chapter"]
        key = prev.chapter_num
    except:
        try:
            curr = vrs_diffs[prev.book][prev.chapter_num]
            key = prev.verse_num
        except:
            return versifications, ruled_out
    try:
        curr_versifications = curr[key].copy()
    except:
        return versifications, ruled_out
    if len(curr_versifications) == 1:
        return curr_versifications, ruled_out
    for num, versifs in curr.items():
        if num != key:
            for versif in versifs:
                if not versif in ruled_out:
                    ruled_out.append(versif)
    to_remove = []
    for versif in curr_versifications:
        if versif in ruled_out:
            to_remove.append(versif)
    for versif in to_remove:
        curr_versifications.remove(versif)
    if curr_versifications and len(curr_versifications) < len(versifications):
        return curr_versifications, ruled_out
    return versifications, ruled_out


def get_book_names(project_folder: Path) -> list[str]:
    """
    gets the book names from the specified bible
    param project_folder: the path to the project folder
    return: a list of book names with their corresponding file names
    """
    names = []
    books = (book for book in listdir(project_folder) if ".usfm" in book)
    for book in books:
        name = re.sub(BOOK_NUM, "", book)
        name = re.sub(EXCLUDE_ALPHANUMERICS, "", name)
        name = re.sub(POST_PART, "", name)
        names.append((name, book))
    return names


def write_temp_settings_file(project_folder: Path, post_part: str) -> None:
    """
    writes a temporary settings file as a place holder to be able to determine the versification
    param project_folder: the path to the project folder
    """
    with open(project_folder / "Settings.xml", "w", encoding="utf-8") as set_file:
        set_file.write(
            f"""<ScriptureText>
            <BiblicalTermsListSetting>Major::BiblicalTerms.xml</BiblicalTermsListSetting>
            <Naming BookNameForm="46-MAT" PostPart="{post_part}.usfm" PrePart="" />
            </ScriptureText>"""
        )


def get_corpus(
    project_folder: Path, vrs_diffs: dict[str, dict[int, dict[int, list[str]]]]
) -> list[tuple[str, VerseRef, VerseRef]]:
    """
    creates a corpus of books found in vrs_diffs
    param project_folder: the path to the project folder
    param vrs_diffs: the list of differences in the versifications
    return: the corpus from the available books in the specified bible
    """
    vrs_path = project_folder / "versification"
    vrs_path.mkdir(parents=True, exist_ok=True)
    book_names = get_book_names(project_folder)
    for name in book_names:
        if name[0] in vrs_diffs.keys():
            shutil.copyfile(project_folder / name[1], vrs_path / name[1])
    write_temp_settings_file(vrs_path, project_folder.name)
    corpus = ParatextTextCorpus(vrs_path)
    lines = list(extract_scripture_corpus(corpus, corpus))
    shutil.rmtree(vrs_path)
    
    return lines


def get_versification(
    project_folder: Path,
    vrs_diffs: dict[str, dict[int, dict[int, list[str]]]],
) -> str:
    """
    gets the versification of the given bible
    param project_folder: the path to the project folder
    param vrs_diffs: the list of differences in the versifications
    return: the versification of the given bible
    """
    lines = get_corpus(project_folder, vrs_diffs)
    versifications = list(vrs_to_num.keys())
    ruled_out = []
    try:
        prev = lines[0][1]
    except:
        return versifications[0]
    for line in lines[1:]:
        vref = line[1]
        try:
            vrs_diffs[prev.book]
        except:
            prev = vref
            continue
        if vref.chapter_num != prev.chapter_num:
            versifications, ruled_out = check_vref(
                prev, vrs_diffs, versifications, ruled_out
            )
            if len(versifications) == 1:
                return versifications[0]
        prev = vref
    versifications, ruled_out = check_vref(prev, vrs_diffs, versifications, ruled_out)
    return versifications[0]


def write_settings_file(project_folder: Path, language_code: str, translation_id: str) -> Path:
    """
    Write a Settings.xml file to the project folder passed if one doesn't exist already.

    The file is very minimal containing only:
      <Versification> (which is inferred from the project)
      <LanguageIsoCode> (which is the first 3 characters of the folder name)
      <Naming> (which is the naming convention "MAT" indicating no digits prior to the 3 letter book code)

    When a settings file is created, the path to it is returned.
    Otherwise None is returned.

    Note that the "Naming->PostPart" section will reflect the original naming scheme of the files in the original zip.
    For example if the original zip was eng-web-c.zip, then the files inside will have names like MATeng-web-c.usfm,
    even though for that language, we would have changed the project name to web_c
    See also ebible.py `create_project_name` method, and rename_usfm.py and
    https://github.com/BibleNLP/ebible/issues/50#issuecomment-2659064715
    """

    # Now add a Settings.xml file to a project folder.
    if project_folder.is_dir():
        settings_file = project_folder / "Settings.xml"

        if settings_file.is_file():
            return None
        else:
            versification = get_versification(project_folder, get_vrs_diffs())
            vrs_num = vrs_to_num[versification]
            setting_file_text = f"""<ScriptureText>
                                <Versification>{vrs_num}</Versification>
                                <LanguageIsoCode>{language_code}:::</LanguageIsoCode>
                                <Naming BookNameForm="MAT" PostPart="{translation_id}.usfm" PrePart="" />
                                </ScriptureText>"""

            with open(settings_file, "w") as settings:
                settings.write(setting_file_text)
            return settings_file


def write_settings_files_orig(base_folder: Path) -> Tuple[int, int]:

    # Add a Settings.xml file to each project folder in a base folder.
    project_folders = [
        project_folder
        for project_folder in base_folder.glob("*")
        if project_folder.is_dir()
    ]
    count_new_settings_files = sum(
        write_settings_file(project_folder) for project_folder in project_folders
    )
    count_existing_settings_files = len(project_folders) - count_new_settings_files

    return count_new_settings_files, count_existing_settings_files
