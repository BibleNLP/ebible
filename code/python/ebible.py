import csv
import os
import re
import shutil
from datetime import datetime
from os import listdir, remove
from pathlib import Path

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from machine.corpora import ParatextTextCorpus, extract_scripture_corpus
from machine.scripture.verse_ref import VerseRef
from tqdm import tqdm

license_headers: list[str] = [
    "ID",
    "Scope",
    "Script",
    "Name",
    "License Type",
    "License Version",
    "License Link",
    "Copyright",
]

versification_to_num: dict[str, int] = {
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


def create_folders(
    base_str: str = "",
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path, Path, Path, Path]:
    """
    creates all the neccessary folders to download the catalog
    param base_str: the name of the base folder
    return: the base, downloads, projects, temp, and logs folders
    """
    path = os.path.realpath(__file__)
    dir = os.path.dirname(path)
    dir = dir.replace("code\python", "")
    os.chdir(dir)
    if not base_str:
        base: Path = Path("output")
    else:
        base = Path(base_str)
    downloads: Path = base / "downloads"
    projects: Path = base / "projects"
    private_projects: Path = base / "private_projects"
    temp: Path = base / "temp"
    logs: Path = base / "logs"
    metadata_folder: Path = Path("metadata")
    corpus: Path = Path("corpus")
    private_corpus: Path = base / "private_corpus"
    public_extract_log: Path = (
        logs / f"extract_public{datetime.now().strftime('%Y_%m_%d-%H_%M')}.log"
    )
    private_extract_log: Path = (
        logs / f"extract_private{datetime.now().strftime('%Y_%m_%d-%H_%M')}.log"
    )
    for dir in [
        base,
        downloads,
        projects,
        private_projects,
        temp,
        logs,
        metadata_folder,
        corpus,
        private_corpus,
        public_extract_log,
        private_extract_log,
    ]:
        if not dir.is_dir():
            dir.mkdir(parents=True, exist_ok=True)
    return (
        base,
        downloads,
        projects,
        private_projects,
        temp,
        logs,
        metadata_folder,
        corpus,
        private_corpus,
        public_extract_log,
        private_extract_log,
    )


def get_vrs_diffs() -> dict[str, dict[int, dict[int, list[str]]]]:
    """
    gets the differences in versifications from vrs_difs.yaml
    return: the versification differences
    """
    with open("code/python/vrs_diffs.yaml", "r") as file:
        try:
            vrs_difs = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)
    return vrs_difs


def download_translations_file(metadata_folder: Path) -> None:
    """
    downloads the translations.csv file
    param metadata_folder: the metadata folder
    """
    r = requests.get("https://ebible.org/Scriptures/translations.csv")
    if r.status_code == requests.codes.ok:
        with open(metadata_folder / "translations.csv", "wb") as out_file:
            out_file.write(r.content)


def get_bibles(metadata_folder: Path) -> tuple[list[str], list[str]]:
    """
    gets all the bibles from https://ebible.org/Scriptures
    param metadata_folder: the metadata folder
    return: a list of all the bibles
    """
    with open(
        metadata_folder / "translations.csv", "r", encoding="utf-8"
    ) as translations:
        reader = csv.DictReader(translations)
        df = pd.DataFrame(reader)
    redist = df[df["Redistributable"].values == "True"]
    non_redist = df[df["Redistributable"].values == "False"]
    return (
        df["translationId"].values,
        redist["translationId"].values,
        non_redist["translationId"].values,
    )


def download_usfx(lang_code: str, temp: Path) -> bool:
    """
    downloads the usfx file
    param lang_code: the language code of the bible
    param temp: the temporary folder
    return: true if the download was successfull, false otherwise
    """
    r = requests.get(f"https://ebible.org/scriptures/{lang_code}_usfx.zip")
    if r.status_code == requests.codes.ok:
        with open(temp / f"{lang_code}_usfx.zip", "wb") as out_file:
            out_file.write(r.content)
        shutil.unpack_archive(
            temp / f"{lang_code}_usfx.zip", temp / f"{lang_code}_usfx"
        )
    return os.path.isdir(temp / f"{lang_code}_usfx")


def get_metadata(lang_code: str, temp: Path) -> BeautifulSoup:
    """
    gets the metadata from the usx file
    param lang_code: the language code of the bible
    param temp: the temporary folder
    return: the metadata
    """
    with open(
        temp / f"{lang_code}_usfx" / f"{lang_code}metadata.xml", "r", encoding="utf-8"
    ) as in_file:
        file = in_file.read()
        return BeautifulSoup(file, "lxml")


def get_book_names(
    lang_code: str, include_file_names: bool, projects: Path
) -> list[str]:
    """
    gets the book names from the specified bible
    param lang_code: the language code of the bible
    param include_file_names: indicates whether to include file names in the list or not
    param projects: the projects folder
    return: a list of book names with their corresponding file names
    """
    names = []
    books = (book for book in listdir(projects / lang_code) if ".usfm" in book)
    for book in books:
        name = re.sub(BOOK_NUM, "", book)
        name = re.sub(EXCLUDE_ALPHANUMERICS, "", name)
        name = re.sub(POST_PART, "", name)
        if include_file_names:
            names.append((name, book))
        else:
            names.append(name)
    return names


def download_bible(lang_code: str, metadata: BeautifulSoup, projects: Path) -> bool:
    """
    checks if the bible should be downloaded
    param lang_code: the language code of the bible
    param metadata: the metadata of the bible
    param projects: the projects folder
    return: true if the bible should be downloaded, false otherwise
    """
    lang_code_path: Path = projects / lang_code
    if lang_code in listdir(projects):
        if [book["code"] for book in metadata.find("books").find_all("book")] == [
            book for book in get_book_names(lang_code, False, projects)
        ]:
            return False
        else:
            for file in listdir(lang_code_path):
                if file != "Settings.xml" and file != "Versification.xml":
                    remove(lang_code_path / file)
    return True


def download_usfm(
    lang_code: str, log: list[str], projects: Path, downloads: Path
) -> list[str]:
    """
    download the usfm file
    param bible: the bible to download the usfm of
    param lang_code: the language code of the bible
    param log: the list of information about downloads
    param projects: the projects folder
    param downloads the downloads folder
    return: the updated log
    """
    zipped = f"{lang_code}.zip"
    r = requests.get(f"https://ebible.org/scriptures/{lang_code}_usfm.zip")
    if r.status_code == requests.codes.ok:
        with open(downloads / zipped, "wb") as out_file:
            out_file.write(r.content)
    shutil.unpack_archive(downloads / zipped, projects / lang_code)
    log.append(f"{datetime.now()}: downloaded {lang_code}\n")
    return log


def write_temp_settings_file(lang_code: str, projects: Path) -> None:
    """
    writes a temporary settings file as a place holder to be able to determine the versification
    param lang_code: the language code of the bible
    param projects: the projects folder
    """
    if not "Settings.xml" in listdir(projects / lang_code):
        with open(
            projects / lang_code / "Settings.xml", "w", encoding="utf-8"
        ) as set_file:
            set_file.write(
                f"""<ScriptureText>
                <BiblicalTermsListSetting>Major::BiblicalTerms.xml</BiblicalTermsListSetting>
                <Naming BookNameForm="46-MAT" PostPart="{lang_code}.usfm" PrePart="" />
                </ScriptureText>"""
            )


def determined_versification(lang_code: str, projects: Path) -> bool:
    """
    checks if the versification has been determined
    param lang_code: the language code of the bible
    param projects: the projects folder
    return: true if the versification has been determined, false otherwise
    """
    try:
        with open(projects / lang_code / "Versification.xml", "r") as set_file:
            contents = set_file.read()
            settings = BeautifulSoup(contents, "lxml")
        det_vrs = settings.find("determinedversification").text
        if det_vrs == "True":
            return True
        return False
    except:
        return False


def check_vref(
    prev: VerseRef,
    vrs_difs: dict[str, dict[int, dict[int, list[str]]]],
    versifications: list[str],
    ruled_out: list[str],
) -> tuple[list[str], list[str]]:
    """
    checks a verse reference to try to determine the versification
    param prev: the verse reference to check
    param vrs_difs: the list of differences in the versifications
    param versifications: the list of possible versifications
    param ruled_out: the list of versifications that have been ruled out
    return: the list of possible versifications and the list of versifications that have been ruled out
    """
    try:
        curr = vrs_difs[prev.book]["last_chapter"]
        key = prev.chapter_num
    except:
        try:
            curr = vrs_difs[prev.book][prev.chapter_num]
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


def get_corpus(
    lang_code: str, vrs_difs: dict[str, dict[int, dict[int, list[str]]]], projects: Path
) -> list[tuple[str, VerseRef, VerseRef]]:
    """
    creates a corpus of books found in vrs_difs
    param lang_code: the language code of the bible
    param vrs_difs: the list of differences in the versifications
    param projects: the projects folder
    return: the corpus from the available books in the specified bible
    """
    lang_code_path = projects / lang_code
    vrs_path = lang_code_path / "versification"
    vrs_path.mkdir(parents=True, exist_ok=True)
    book_names = get_book_names(lang_code, True, projects)
    for name in book_names:
        if name[0] in vrs_difs.keys():
            shutil.copyfile(lang_code_path / name[1], vrs_path / name[1])
    write_temp_settings_file(lang_code, projects)
    shutil.copyfile(lang_code_path / "Settings.xml", vrs_path / "Settings.xml")
    corpus = ParatextTextCorpus(vrs_path)
    lines = list(extract_scripture_corpus(corpus, corpus))
    shutil.rmtree(vrs_path)
    return lines


def get_versification(
    lang_code: str,
    vrs_difs: dict[str, dict[int, dict[int, list[str]]]],
    projects: Path,
) -> tuple[str, list[str]]:
    """
    gets the versification of the given bible
    param lang_code: the language code of the bible
    param vrs_difs: the list of differences in the versifications
    param log: the list of information about downloads
    param projects: the projects folder
    return: the versification and the updated log
    """
    lines = get_corpus(lang_code, vrs_difs, projects)
    versifications = [
        "English",
        "Original",
        "Russian Protestant",
        "Russian Orthodox",
        "Septuagint",
        "Vulgate",
    ]
    ruled_out = []
    try:
        prev = lines[0][1]
    except:
        return versifications[0], versifications
    for line in lines[1:]:
        vref = line[1]
        try:
            vrs_difs[prev.book]
        except:
            prev = vref
            continue
        if vref.chapter_num != prev.chapter_num:
            versifications, ruled_out = check_vref(
                prev, vrs_difs, versifications, ruled_out
            )
            if len(versifications) == 1:
                return versifications[0], versifications
        prev = vref
    versifications, ruled_out = check_vref(prev, vrs_difs, versifications, ruled_out)
    return versifications[0], versifications


def write_settings_file(lang_code: str, versification: str, projects: Path) -> None:
    """
    writes the settings file for the given bible
    param lang_code: the language code of the bible
    param versification: the versification of the bible
    param det_versif: the result of trying to get the versification
    param projects: the projects folder
    """
    with open(projects / lang_code / "Settings.xml", "w", encoding="utf-8") as set_file:
        set_file.write(
            f"""<ScriptureText>
            <Versification>{versification_to_num[versification]}</Versification>
            <LanguageIsoCode>{lang_code.split("_")[0]}:::</LanguageIsoCode>
            <BiblicalTermsListSetting>Major::BiblicalTerms.xml</BiblicalTermsListSetting>
            <Naming BookNameForm="46-MAT" PostPart="{lang_code}.usfm" PrePart="" />
            </ScriptureText>"""
        )


def write_versification_file(
    lang_code: str,
    versification: str,
    versifications: list[str],
    log: list[str],
    projects: Path,
):
    """
    writes the versification file for the given bible
    param lang_code: the language code of the bible
    param versification: the versification of the bible
    param versifications: the possible versifications of the bible
    param log: the list of information about downloads
    param projects: the projects folder
    return: the updated log
    """
    if len(versifications) > 1:
        det_vrs = False
        log.append(
            f"{lang_code}: could not determine versification, guessed {versification} from {versifications}\n"
        )
    else:
        det_vrs = True
    with open(
        projects / lang_code / "Versification.xml", "w", encoding="utf-8"
    ) as vrs_file:
        vrs_file.write(
            f"""<Data>
            <DeterminedVersification>{det_vrs}</DeterminedVersification>
            <Versification>{versification}</Versification>
            <GuessedFrom>{versifications}</GuessedFrom>
            </Data>"""
        )
    return log


def create_license_entry(
    lang_code: str,
    metadata: BeautifulSoup,
    licenses: list[dict[str, str]],
    projects: Path,
) -> list[dict[str, str]]:
    """
    creates a license entry for the given bible
    param lang_code: the language code of the bible
    param metadata: the metadata of the bible
    param licenses: the list of license entries
    param projects: the projects folder
    return: the updated list of license entries
    """
    with open(projects / lang_code / "copr.htm", "r", encoding="utf-8") as copr_file:
        html = copr_file.read()
        copr = BeautifulSoup(html, "html.parser")
    para_elems = copr.find_all("p")
    license_type = ""
    for para in para_elems:
        try:
            link = para.a.get("href")
            if (
                "https://creativecommons.org/licenses/" in link
                or "http://creativecommons.org/licenses/" in link
            ):
                license_link = link
                break
            para = para.text
            if "Public Domain" in para:
                license_type = "Public Domain"
                break
        except:
            pass
    try:
        data: list = license_link.split("/")
        license_type = data[4]
        license_ver = data[5]
    except:
        license_ver = ""
        license_link = ""
    lang_elem = metadata.find("language")
    entry: dict[str, str] = dict.fromkeys(license_headers)
    entry["ID"] = lang_code
    entry["Scope"] = metadata.find("identification").scope.text
    entry["Script"] = lang_elem.script.text
    entry["Name"] = lang_elem.find("name").text
    entry["License Type"] = license_type
    entry["License Version"] = license_ver
    entry["License Link"] = license_link
    entry["Copyright"] = metadata.find("copyright").text.strip().replace("\n", "\t")
    licenses.append(entry)
    return licenses


def write_licenses_file(licenses: list[dict[str, str]], metadata_folder: Path) -> None:
    """
    writes the licenses file
    param licenses: the list of licenses
    param base: the base folder
    """
    with open(
        metadata_folder / "licenses.csv", "w", newline="", encoding="utf-8"
    ) as license_file:
        writer = csv.DictWriter(license_file, fieldnames=license_headers)
        writer.writeheader()
        for license in licenses:
            writer.writerow(license)


def clean_folders(
    catalog: list[str], log: list[str], projects: Path, downloads: Path, temp: Path
) -> list[str]:
    """
    deletes the temporary folder and any bibles that are no longer in the catalog
    param catalog: the catalog of available bibles
    param log: the list of information about downloads
    param projects: the projects folder
    param downloads: the downloads folder
    param temp: the temporary folder
    return: the updated log
    """
    shutil.rmtree(temp)
    for lang_code in listdir(projects):
        if not lang_code in catalog:
            log.append(f"{lang_code} is no longer in the catalog\n")
            try:
                shutil.rmtree(projects / lang_code)
            except:
                log.append(
                    f"{datetime.now()}: unable to remove {lang_code} from the projects folder\n"
                )
            try:
                remove(downloads / f"{lang_code}.zip")
            except:
                log.append(
                    f"{datetime.now()}: unable to remove {lang_code} from the downloads folder\n"
                )
            log.append(f"{datetime.now()}: removed {lang_code}\n")
    return log


def write_log_file(log: list[str], logs: Path) -> None:
    """
    writes the log file
    param log: the list of information about downloads
    param logs: the logs folder
    """
    with open(
        logs / f"{datetime.now().strftime('%Y_%m_%d-%H_%M')}.log", "w", encoding="utf-8"
    ) as log_file:
        for message in log:
            log_file.write(message)
        log_file.write("all files up to date")


def main() -> None:
    """
    downloads all the translations from https://ebible.org/Scriptures
    """
    (
        base,
        downloads,
        projects,
        private_projects,
        temp,
        logs,
        metadata_folder,
        corpus,
        private_corpus,
        public_extract_log,
        private_extract_log,
    ) = create_folders()
    vrs_difs = get_vrs_diffs()
    download_translations_file(metadata_folder)
    catalog, public_bibles, private_bibles = get_bibles(metadata_folder)
    licenses: list[dict[str, str]] = []
    log: list[str] = []
    print("downloading the catalog...")
    for lang_code in tqdm(catalog):
        if lang_code in public_bibles:
            projects_folder = projects
        else:
            projects_folder = private_projects
        if download_usfx(lang_code, temp):
            metadata = get_metadata(lang_code, temp)
            if download_bible(lang_code, metadata, projects_folder):
                log = download_usfm(lang_code, log, projects_folder, downloads)
                if not determined_versification(lang_code, projects_folder):
                    versification, versifications = get_versification(
                        lang_code, vrs_difs, projects_folder
                    )
                    write_settings_file(lang_code, versification, projects_folder)
                    log = write_versification_file(
                        lang_code, versification, versifications, log, projects_folder
                    )
            licenses = create_license_entry(
                lang_code, metadata, licenses, projects_folder
            )
        break
    write_licenses_file(licenses, metadata_folder)
    log = clean_folders(public_bibles, log, projects, downloads, temp)
    write_log_file(log, logs)
    print("Use this command to extract the public_projects to the public_corpus.")
    print(
        f"poetry run python -m silnlp.common.bulk_extract_corpora --input {projects} --output {corpus} --error-log {public_extract_log}"
    )
    print("Use this command to extract the private_projects to the private_corpus.")
    print(
        f"poetry run python -m silnlp.common.bulk_extract_corpora --input {private_projects} --output {private_corpus} --error-log {private_extract_log}"
    )


if __name__ == "__main__":
    main()
