"""ebible.py contains functions to:
Download zipped Bibles in USFM format from ebible.org to the 'downloads' folder.
Unzip the downloaded files to the 'projects' folder.
Check the licence data contained in the copr.htm files in the redistributable_folder folders. Write that as a csv file.
Check the existence of verses in certain chapters in order to guess the versification. Add that to a Settings.xml in each project.
The Settings.xml file is necessary for silnlp.common.extract_corpora.
Extract files that have a licence that permits redistibution using SILNLP for the extraction.
"""

# Import modules and directory paths
import argparse
import codecs
import ntpath
import os
import re
import shutil
from csv import DictReader, DictWriter
from datetime import datetime
from glob import iglob
from os import listdir
from pathlib import Path
from random import randint
from time import sleep, strftime
from typing import Dict, List, Tuple
from settings_file import write_settings_files


import pandas as pd
import regex
import requests
from bs4 import BeautifulSoup
from pandas.core.groupby import groupby

global headers
headers: Dict[str, str] = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0",
}

# Define methods for downloading and unzipping eBibles


def log_and_print(file, s, type="Info") -> None:

    with open(file, "a") as log:
        log.write(
            f"{type.upper()}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {s}\n"
        )
    print(s)


def make_directories(dirs_to_create) -> None:
    for dir_to_create in dirs_to_create:
        dir_to_create.mkdir(parents=True, exist_ok=True)


def download_file(url, file, headers=headers):

    r = requests.get(url, headers=headers)
    # If the status is OK continue
    if r.status_code == requests.codes.ok:

        with open(file, "wb") as out_file:
            # Write out the content of the page.
            out_file.write(r.content)

        return True
    return False


def download_files(filenames, base_url, folder, logfile, redownload=False) -> None:

    downloaded = 0

    for i, filename in enumerate(filenames):

        # Construct the download url and the local file path.
        url = base_url + filename
        file = folder / filename

        # Skip any missing filenames.
        if filename == "":
            continue

        # Skip existing files that contain data.
        elif file.exists() and file.stat().st_size > 100:
            log_and_print(
                logfile,
                f"{i+1}: {file} already exists and contains {file.stat().st_size} bytes.",
            )

            if redownload:
                log_and_print(logfile, f"{i+1}: Redownloading from {url} to {file}.")
                done = download_file(url, file)

                if done:
                    downloaded += 1
                    log_and_print(logfile, f"Saved {url} as {file}\n")
                    # Pause for a random number of miliseconds
                    pause = randint(1, 5000) / 1000
                    sleep(pause)
            continue

        else:
            log_and_print(logfile, f"{i+1}: Downloading from {url} to {file}.")
            done = download_file(url, file)

            if done:
                downloaded += 1
                log_and_print(logfile, f"Saved {url} as {file}\n")
                # Pause for a random number of miliseconds
                pause = randint(1, 5000) / 1000
                sleep(pause)

            else:
                log_and_print(logfile, f"Could not download {url}\n")

    log_and_print(
        logfile,
        f"\nFinished downloading. Downloaded {downloaded} zip files from eBbile.org",
    )


def get_tree_size(path) -> int:
    """Return total size of files in given path and subdirs."""
    total: int = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            total += get_tree_size(entry.path)
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total


def unzip_ebible(source_file, dest_folder) -> None:

    if dest_folder.is_dir():
        log_and_print(logfile, f"Extracting {source_file} to: {dest_folder}")
        shutil.unpack_archive(source_file, dest_folder)
        # log_and_print(f"Extracted {source_file} to: {dest_folder}")

    else:
        log_and_print(logfile, f"Can't extract, {dest_folder} doesn't exist.")


def unzip_entire_folder(source_folder, file_suffix, unzip_folder, logfile) -> int:
    log_and_print(logfile, f"\nStarting unzipping eBible zip files...")
    pattern = "*" + file_suffix
    zip_files = sorted([zip_file for zip_file in source_folder.glob(pattern)])
    log_and_print(
        logfile,
        f"Found {len(zip_files)} files in {source_folder} matching pattern: {pattern}",
    )

    # Strip off the pattern so that the subfolder name is the project ID.
    extract_folders = [
        (
            zip_file,
            unzip_folder
            / f"{zip_file.name[0: (len(zip_file.name) - len(file_suffix))]}",
        )
        for zip_file in zip_files
    ]
    extracts = [
        (zip_file, folder)
        for zip_file, folder in extract_folders
        if not folder.exists()
    ]

    log_and_print(
        logfile,
        f"Found {len(extracts)} that were not yet extracted.\n",
    )

    for zip_file, extract in extracts:
        extract.mkdir(parents=True, exist_ok=True)
        log_and_print(logfile, f"Extracting to: {extract}")
        shutil.unpack_archive(zip_file, extract)

    # log_and_print(logfile, f"Finished unzipping eBible files\n")

    return len(extracts)


def unzip_files(zip_files, file_suffix, unzip_folder, logfile, dist_type) -> int:

    # Strip off the file_suffix so that the subfolder name is the project ID.
    unzip_to_folders = [
        (
            zip_file,
            unzip_folder
            / f"{zip_file.name[0: (len(zip_file.name) - len(file_suffix))]}",
        )
        for zip_file in zip_files
    ]
    unzips = [
        (zip_file, folder)
        for zip_file, folder in unzip_to_folders
        if not folder.exists()
    ]

    unzipped_count: int = len(unzips)

    if unzips:
        log_and_print(
            logfile,
            f"Found {len(unzips)} files that haven't been unzipped to {unzip_folder}",
        )

        for zip_file, unzip_to_folder in unzips:
            unzip_to_folder.mkdir(parents=True, exist_ok=True)
            log_and_print(logfile, f"Extracting to: {unzip_to_folder}")
            try:
                shutil.unpack_archive(zip_file, unzip_to_folder)
            except shutil.ReadError:
                unzipped_count -= 1
                log_and_print(logfile, f"ReadError: While trying to unzip: {zip_file}")
            except FileNotFoundError:
                unzipped_count -= 1
                log_and_print(
                    logfile, f"FileNotFoundError: While trying to unzip: {zip_file}"
                )

        log_and_print(
            logfile,
            f"Sucessfully unzipped {unzipped_count} eBible files to {unzip_folder}",
        )
        return unzipped_count

    else:
        log_and_print(
            logfile,
            f"All {len(zip_files)} {dist_type} zip files have already been unzipped to {unzip_folder}.",
        )

    return unzipped_count


def get_redistributable(translations_csv: Path) -> Tuple[List[Path], List[Path]]:

    redistributable_files: List = []
    all_files: List = []

    with open(translations_csv, encoding="utf-8-sig", newline="") as csvfile:
        reader = DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            all_files.append(row["translationId"])

            if row["Redistributable"] == "True":
                redistributable_files.append(row["translationId"])

        return all_files, redistributable_files


# Define methods for creating the copyrights file


def norm_name(name: str) -> str:
    if len(name) < 3:
        return None
    elif len(name) == 3:
        return f"{name[:3]}-{name}"
    elif len(name) >= 7 and name[3] == "-" and name[0:3] == name[4:7]:
        return name
    else:
        return f"{name[:3]}-{name}"


def get_download_lists(
    translations_csv: Path,
    file_suffix: str,
    download_folder: Path,
    wont_download: List[str],
) -> Tuple[List[Path], List[Path], List[Path], List[Path], List[Path]]:

    # It's actually too soon to decide which files are redistributable and which are not
    # We should unzip all first, then compare the translations_csv info with that
    # scraped from the copr.htm files.

    # Get filenames
    all_files, redistributable_files = get_redistributable(translations_csv)
    non_redistributable_files = sorted(set(all_files) - set(redistributable_files))

    all_filenames = sorted([file + file_suffix for file in all_files])

    # Find which files have already been downloaded.
    already_downloaded = sorted(
        [file.name for file in download_folder.glob("*" + file_suffix)]
    )

    # For downloading we need to maintain the eBible names e.g. aai_usfm.zip
    # print(all_filenames[:3])
    # print(already_downloaded[:3])
    # print(wont_download[:3])
    # print(resticted_filenames[:3])
    # exit()

    dont_download = set(already_downloaded).union(set(wont_download))
    to_download = sorted(set(all_filenames) - set(dont_download))

    return (
        all_files,
        redistributable_files,
        non_redistributable_files,
        to_download,
        already_downloaded,
    )


# Define functions to calculate versification and write the settings file.

# Columns are easier to use if they are valid python identifiers:
def improve_column_names(df) -> None:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace('"', "")
        .str.replace("'", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace(" ", "_")
    )


def write_licence_file(licence_file, logfile, redistributable_folder):

    csv_headers = [
        "ID",
        "Language",
        "Dialect",
        "Licence Type",
        "Licence Version",
        "CC Licence Link",
        "Copyright Holder",
        "Copyright Years",
        "Translation by",
    ]

    # Get copyright info from eBible projects

    data = list()
    copr_regex = r".*[/\\](?P<id>.*?)[/\\]copr.htm"

    log_and_print(logfile, "Collecting eBible copyright information...")

    for i, copyright_file in enumerate(
        sorted(redistributable_folder.glob("**/copr.htm"))
    ):
        id = str(copyright_file.parents[0].relative_to(redistributable_folder))

        id_match = regex.match(copr_regex, str(copyright_file))
        id = id_match["id"]

        if i % 100 == 0:
            print(f"Read {i} files. Now reading: {copyright_file} with ID: {id}")

        entry = dict()
        entry["ID"] = str(id)
        entry["File"] = copyright_file

        with open(copyright_file, "r", encoding="utf-8") as copr:
            html = copr.read()
            soup = BeautifulSoup(html, "lxml")

        cclink = soup.find(href=regex.compile("creativecommons"))
        if cclink:
            ref = cclink.get("href")
            if ref:
                entry["CC Licence Link"] = ref
                cc_match = regex.match(
                    r".*?/licenses/(?P<type>.*?)/(?P<version>.*)/", ref
                )
                if cc_match:
                    entry["Licence Type"] = cc_match["type"]
                    entry["Licence Version"] = cc_match["version"]
                else:
                    cc_by_match = regex.match(r".*?/licenses/by(?P<version>.*)/", ref)
                    if cc_by_match:
                        # print(f'Licence version = {cc_by_match["version"]}')
                        entry["Licence Type"] = "by"
                        entry["Licence Version"] = cc_by_match["version"]

        cclink = None

        titlelink = soup.find(href=regex.compile(f"https://ebible.org/{id}"))
        if titlelink:
            entry["Vernacular Title"] = titlelink.string
        titlelink = None

        copy_strings = [s for s in soup.body.p.stripped_strings]

        for i, copy_string in enumerate(copy_strings):
            if i == 0 and "copyright ©" in copy_string:
                entry["Copyright Years"] = copy_string
                entry["Copyright Holder"] = copy_strings[i + 1]
            if i > 0 and "Language:" in copy_string:
                entry["Language"] = copy_strings[i + 1]

            if "Dialect" in copy_string:
                descriptions = ["Dialect (if applicable): ", "Dialect: "]
                for description in descriptions:
                    if copy_string.startswith(description):
                        entry["Dialect"] = copy_string[len(description) :]
                        break
                    else:
                        entry["Dialect"] = copy_string

            if "Translation by" in copy_string:
                entry["Translation by"] = copy_string
            if "Public Domain" in copy_string:
                entry["Copyright Year"] = ""
                entry["Copyright Holder"] = "Public Domain"

        data.append(entry)

    with open(licence_file, "w", encoding="utf-8", newline="") as csv_out:
        writer = DictWriter(
            csv_out,
            csv_headers,
            restval="",
            extrasaction="ignore",
            dialect="excel",
        )
        writer.writeheader()
        writer.writerows(data)

    log_and_print(
        logfile, f"Wrote licence info for {len(data)} translations to {licence_file}\n"
    )


def choose_yes_no(prompt: str) -> bool:

    choice: str = " "
    while choice not in ["n", "y"]:
        choice: str = input(prompt).strip()[0].lower()
    if choice == "y":
        return True
    elif choice == "n":
        return False



def main() -> None:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Download, unzip and extract text corpora from eBible."
    )
    parser.add_argument(
        "-d",
        "--force_download",
        default=False,
        action="store_true",
        help="Set this flag to overwrite all previous data and start again.",
    )
    parser.add_argument(
        "-s",
        "--overwrite_settings",
        default=False,
        action="store_true",
        help="Set this flag to overwrite the settings.xml files.",
    )
    parser.add_argument(
        "-e",
        "--overwrite_extracts",
        default=False,
        action="store_true",
        help="Set this flag to overwrite the extracted files.",
    )
    parser.add_argument(
        "-l",
        "--overwrite_licences",
        default=False,
        action="store_true",
        help="Set this flag to overwrite the licences.tsv file.",
    )
    parser.add_argument("folder", help="The base folder where others will be created.")

    args: argparse.Namespace = parser.parse_args()
    # print(args, type(args))
    # exit()

    # Define base folder
    base: Path = Path(args.folder)

    translations_csv_url: str = r"https://ebible.org/Scriptures/translations.csv"
    eBible_url: str = r"https://ebible.org/Scriptures/"
    file_suffix: str = "_usfm.zip"

    # The corpus folder is for the verse aligned text files.
    corpus_folder: Path = base / "corpus"

    # The redistributable_folder folder is for the redistributable_folder downloaded files.
    # To this folder we add a settings.xml file that we create.
    # The settings.xml file is required for the extraction process.
    redistributable_folder: Path = base / "projects"

    # Folder for the non-redistributable projects.
    non_redistributable_folder: Path = base / "private_projects"

    # The zipped folder is where we download files from eBible.org
    downloads_folder: Path = base / "downloads"

    metadata_folder: Path = base / "metadata"

    logs_folder: Path = base / "logs"

    year, month, day, hour, minute = map(int, strftime("%Y %m %d %H %M").split())
    log_suffix: str = f"_{year}_{month}_{day}-{hour}_{minute}.log"
    log_filename: str = "ebible" + log_suffix
    logfile: Path = logs_folder / log_filename

    # Only used in the final command hint shown to the user.
    # This helps to keep the datestamps of the two logs in sync.
    extract_log_file: Path = logs_folder / ("extract" + log_suffix)

    # The file to download from eBible.org
    translations_csv: Path = metadata_folder / "translations.csv"

    # The file we will save that contains the licence information for each file.
    licence_file: Path = metadata_folder / "licences.tsv"

    all_folders: List = [
        corpus_folder,
        redistributable_folder,
        non_redistributable_folder,
        downloads_folder,
        metadata_folder,
        logs_folder,
    ]
    missing_folders: List = [folder for folder in all_folders if not folder.is_dir()]

    # Don't log_and_print until we've checked that the log folder exists.
    print(f"The base folder is : {base}")
    # base = r"F:/GitHub/davidbaines/eBible"

    if missing_folders:
        print(
            "The following {len(missing_folders)} folders are required and will be created if you continue."
        )
        for folder in missing_folders:
            print(folder)

        print(f"\n\nAre you sure this is the right folder:    {base} ")
        if choose_yes_no(f"Enter Y to continue or N to Quit."):

            # Create the required directories
            make_directories(missing_folders)
            log_and_print(logfile, f"All the required folders were created at {base}\n")
        else:
            exit()

    else:
        log_and_print(logfile, f"All the required folders exist at {base}\n")

    if not translations_csv.is_file() or args.force_download:
        # Download the list of translations.
        log_and_print(
            logfile,
            f"Downloading list of translations from {translations_csv_url} to: {str(translations_csv)}",
        )
        download_file(translations_csv_url, translations_csv)

    # These wont download usually.
    wont_download: List[str] = [
        "due_usfm.zip",
        "engamp_usfm.zip",
        "engnasb_usfm.zip",
        "khm-h_usfm.zip",
        "khm_usfm.zip",
        "sancol_usfm.zip",
        "sankan_usfm.zip",
        "spaLBLA_usfm.zip",
        "spanblh_usfm.zip",
    ]

    # These are copyright and should be shared.
    private_zipfiles: List[str] = [
        "bbr2013",
        "big2013",
        "fad",
        "ino2013",
        "kud2014",
        "kxw",
        "nuq-nuq",
    ]

    # These can be shared.
    public_zipfiles: List[str] = [
        "bzj",
        "cmeNT",
        "cotNT",
        "urd",
    ]

    (
        all_files,
        redistributable_files,
        non_redistributable_files,
        to_download,
        already_downloaded,
    ) = get_download_lists(
        translations_csv, file_suffix, downloads_folder, wont_download=wont_download
    )

    redistributable_files += public_zipfiles
    redistributable_zipfiles: List(Path) = sorted(
        [downloads_folder / (file + file_suffix) for file in redistributable_files]
    )

    non_redistributable_files += private_zipfiles
    non_redistributable_zipfiles: List(Path) = sorted(
        [downloads_folder / (file + file_suffix) for file in non_redistributable_files]
    )

    # print(f"The first redistributable file is {redistributable_files[0]}")
    # print(f"The first non redistributable file is {non_redistributable_files[0]}")

    log_and_print(
        logfile,
        f"The translations csv file lists {len(all_files)} translations and {len(redistributable_files)} are redistributable.",
    )
    log_and_print(
        logfile,
        f"The translations csv file lists {len(non_redistributable_files)} non redistributable translations.",
    )
    # log_and_print(
    #    logfile,
    #    f"{len(non_redistributable_files)} + {len(redistributable_files)} = {len(non_redistributable_files) + len(redistributable_files)}",
    # )
    log_and_print(
        logfile,
        f"There are {len(already_downloaded)} files with the suffix {file_suffix} already in {downloads_folder}",
    )
    log_and_print(
        logfile, f"There are {len(wont_download)} files that usually fail to download."
    )
    if to_download:
        log_and_print(logfile, f"There are {len(to_download)} files still to download.")
    else:
        log_and_print(logfile, f"All zip files have already been downloaded.")

    # Download the required zipped USFM files.
    download_files(
        to_download,
        eBible_url,
        downloads_folder,
        logfile,
        redownload=args.force_download,
    )

    # print(f"Redist files = {redistributable_zipfiles}, {type(redistributable_zipfiles)}")
    # print(f"Non redist files = {non_redistributable_zipfiles}, {type(non_redistributable_zipfiles)}")

    # Unzip the redistributable downloaded files.
    redistributable_project_count: int = unzip_files(
        redistributable_zipfiles,
        file_suffix,
        redistributable_folder,
        logfile,
        "redistributable",
    )

    # Unzip the non_redistributable downloaded files to the private_projects folder.
    unzip_files(
        non_redistributable_zipfiles,
        file_suffix,
        non_redistributable_folder,
        logfile,
        "non-redistributable",
    )

    # print(f"{non_redistributable_files[0]} , {type(non_redistributable_files)}")

    # Check whether any non_redistributable files are in the redistributable folder and list them.
    incorrect_non_redistributable_folders: List(Path) = [
        redistributable_folder / non_redistributable_file
        for non_redistributable_file in non_redistributable_files
    ]
    incorrect_non_redistributable_folders: List(Path) = [
        folder for folder in incorrect_non_redistributable_folders if folder.is_dir()
    ]
    if incorrect_non_redistributable_folders:
        log_and_print(
            logfile,
            f"There are {len(incorrect_non_redistributable_folders)} non_redistributable projects in the redistributable project folder: {redistributable_folder}",
        )

        if choose_yes_no("Delete them?  Y / N ?"):
            for (
                incorrect_non_redistributable_folder
            ) in incorrect_non_redistributable_folders:
                log_and_print(
                    logfile,
                    f"Deleting {len(incorrect_non_redistributable_folders)} non_redistributable projects from the redistributable project folder.",
                )
                shutil.rmtree(incorrect_non_redistributable_folder, ignore_errors=True)

    if redistributable_project_count:
        # Write the licence file.
        write_licence_file(licence_file, logfile, redistributable_folder)
    else:
        log_and_print(logfile, f"There are no changes required to the licence file.")

    # Create Settings files for the redistributable projects.
    new_settings_file_count, exsiting_settings_file_count = write_settings_files(
        redistributable_folder
    )

    log_and_print(
        logfile,
        f"\nCreated {new_settings_file_count} Settings.xml files folder: {redistributable_folder}. There are {exsiting_settings_file_count} projects with existing settings files.",
    )

    # Create Settings files for the redistributable projects.
    new_settings_file_count, exsiting_settings_file_count = write_settings_files(
        non_redistributable_folder
    )

    log_and_print(
        logfile,
        f"\nCreated {new_settings_file_count} Settings.xml files folder: {non_redistributable_folder}. There are {exsiting_settings_file_count} projects with existing settings files.",
    )

    # TO DO: Use silnlp.common.extract_corpora to extract all the project files.
    # If silnlp becomes pip installable then we can do that here with silnlp as a dependency.

    log_and_print(
        logfile,
        f"\nUse this command ONLY if you want to extract the non_redistributable files.",
    )
    log_and_print(
        logfile,
        f"poetry run python -m silnlp.common.bulk_extract_corpora --input {non_redistributable_folder} --output <OUTPUT_FOLDER>",
    )

    log_and_print(
        logfile,
        f"\nThe files are ready for extracting. Use this command in the SILNLP repo to extract the redistributable files.",
    )
    log_and_print(
        logfile,
        f"poetry run python -m silnlp.common.bulk_extract_corpora --input {redistributable_folder} --output {corpus_folder} --error-log {extract_log_file}",
    )


if __name__ == "__main__":
    main()
