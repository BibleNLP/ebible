"""ebible.py contains functions to:
Download zipped Bibles in USFM format from ebible.org to the 'downloads' folder.
Unzip the downloaded files to the 'projects' folder.
Check the licence data contained in the copr.htm files in each project folder.
Write the licence information as a csv file.
Check the existence of verses in certain chapters in order to guess the versification. Add that to a Settings.xml in each project.
A Settings.xml file is necessary for silnlp.common.extract_corpora.
Extract all the files saving those that have an open licence to the 'corpus' folder and those that don't to the private_corpus folder.
"""

import argparse
import os
import shutil

# Import modules and directory paths
from csv import DictReader, DictWriter
from datetime import datetime
from glob import iglob
from os import listdir
from pathlib import Path
from random import randint
from time import sleep, strftime
from typing import Dict, List, Tuple

import pandas as pd
import regex
import requests
import yaml
from bs4 import BeautifulSoup
from pandas.core.groupby import groupby

from settings_file import write_settings_file

global headers
headers: Dict[str, str] = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0",
}


# Define methods for downloading and unzipping eBibles
def log_and_print(file, messages, type="Info") -> None:

    if isinstance(messages, str):
        with open(file, "a") as log:
            log.write(
                f"{type.upper()}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {messages}\n"
            )
            print(messages)

    if isinstance(messages, list):
        with open(file, "a") as log:
            for message in messages:
                log.write(
                    f"{type.upper()}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}\n"
                )
                print(message)


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

        return file
    return None


def download_files(files, base_url, folder, logfile, redownload=False) -> list:

    downloaded_files = []

    for i, file in enumerate(files):

        # Construct the download url and the local file path.
        url = base_url + file.name
        file = folder / file.name

        # Skip existing files that contain data.
        if file.exists() and file.stat().st_size > 100:

            if redownload:
                log_and_print(logfile, f"{i+1}: Redownloading from {url} to {file}.")
                if downloaded_file := download_file(url, file):
                    downloaded_files.append(downloaded_file)

                    log_and_print(logfile, f"Saved {url} as {file}\n")
                    # Pause for a random number of miliseconds
                    sleep(randint(1, 5000) / 1000)

            continue

        else:
            log_and_print(logfile, f"{i+1}: Downloading from {url} to {file}.")
            if downloaded_file := download_file(url, file):
                downloaded_files.append(downloaded_file)

                log_and_print(logfile, f"Saved {url} as {file}\n")

                # Pause for a random number of miliseconds
                sleep(randint(1, 5000) / 1000)

            else:
                log_and_print(logfile, f"Could not download {url}\n")

    log_and_print(
        logfile,
        f"\nFinished downloading. Downloaded {len(downloaded_files)} zip files from eBbile.org",
    )
    return downloaded_files


def get_tree_size(path) -> int:
    """Return total size of files in given path and subdirs."""
    total: int = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            total += get_tree_size(entry.path)
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total


def unzip_ebible(source_file, dest_folder, logfile) -> None:

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


def unzip_files(
    zip_files: List[Path], unzip_folder: Path, file_suffix: str, logfile
) -> List[Path]:

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

    unzipped = []

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
                unzipped.append(unzip_to_folder)
            except shutil.ReadError:
                log_and_print(logfile, f"ReadError: While trying to unzip: {zip_file}")
            except FileNotFoundError:
                log_and_print(
                    logfile, f"FileNotFoundError: While trying to unzip: {zip_file}"
                )

    return unzipped


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


def write_licence_file(licence_file, logfile, folder):

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

    for i, copyright_file in enumerate(sorted(folder.glob("**/copr.htm"))):
        id = str(copyright_file.parents[0].relative_to(folder))

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


def check_folders_exist(folders: list, base: Path, logfile):
    missing_folders: List = [folder for folder in folders if not folder.is_dir()]

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
        log_and_print(logfile, f"All the required folders exist in {base}")


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

    # Process outline is:
    #  Download the translation.csv file to the 'metadata' folder.
    #  Read it to see which files need to be downloaded.
    #  Download to 'downloads'
    #  Unzip to 'projects'
    #  Write Settings.xml for each project.
    #  Read licence for each project
    #  Write out a licence.tsv file to the 'metadata' folder.
    #  SILNLP Extract the shareable projects to the 'corpus' folder.
    #  SILNLP Extract the non-shareable projects to the 'private_corpus' folder.

    # The corpus folder is for the verse aligned text files.

    corpus_folder: Path = base / "corpus"
    downloads_folder: Path = base / "downloads"
    private_corpus_folder: Path = base / "private_corpus"
    projects_folder: Path = base / "projects"
    metadata_folder: Path = base / "metadata"
    logs_folder: Path = base / "logs"

    # The csv file to download from eBible.org
    translations_csv: Path = metadata_folder / "translations.csv"

    # Date stamp for the log file.
    year, month, day, hour, minute = map(int, strftime("%Y %m %d %H %M").split())
    log_suffix: str = f"_{year}_{month}_{day}-{hour}_{minute}.log"
    log_filename: str = "ebible" + log_suffix
    logfile: Path = logs_folder / log_filename

    # Only used in the final command hint shown to the user.
    # This helps to keep the datestamps of the two logs in sync.
    extract_log_file: Path = logs_folder / ("extract" + log_suffix)

    # The file we will save that contains the licence information for each file.
    licence_file: Path = metadata_folder / "licences.tsv"

    check_folders_exist(
        [
            corpus_folder,
            downloads_folder,
            private_corpus_folder,
            projects_folder,
            metadata_folder,
            logs_folder,
        ],
        base,
        logfile,
    )

    # Download the list of translations if necessary.
    if not translations_csv.is_file() or args.force_download:
        log_and_print(
            logfile,
            f"Downloading list of translations from {translations_csv_url} to: {str(translations_csv)}",
        )
        download_file(translations_csv_url, translations_csv)
    else:
        log_and_print(
            logfile, f"translations.csv file already exists in: {str(translations_csv)}"
        )

    # Get the exceptions from the config.yaml file.
    with open("config.yaml", "r") as yamlfile:
        config: Dict = yaml.safe_load(yamlfile)

    dont_download_filenames = [
        project + "_usfm.zip" for project in config["No Download"]
    ]
    dont_download_files = [
        downloads_folder / dont_download_filename
        for dont_download_filename in dont_download_filenames
    ]
    private = config["Private"]
    public = config["Public"]

    # Get download file IDs from translations.csv file.
    ebible_file_ids, redistributable_files = get_redistributable(translations_csv)
    ebible_filenames = [file_id + file_suffix for file_id in ebible_file_ids]
    ebible_files = [
        downloads_folder / ebible_filename for ebible_filename in ebible_filenames
    ]
    existing_ebible_files = [
        ebible_file for ebible_file in ebible_files if ebible_file.is_file()
    ]
    previous_ebible_files = [
        file
        for file in downloads_folder.glob("*" + file_suffix)
        if file not in ebible_files
    ]
    files_to_download = (
        set(ebible_files) - set(existing_ebible_files) - set(dont_download_files)
    )
    print(files_to_download)

    # Presumably any other files used to be in eBible and are no longer

    print(
        f"Of {len(ebible_files)} ebible files, {len(existing_ebible_files)} are already downloaded and {len(dont_download_files)} are excluded."
    )
    if previous_ebible_files:
        print(
            f"These {len(previous_ebible_files)} files are already in the download folder, but are no longer listed in translations.csv:"
        )
        for i, previous_ebible_file in enumerate(previous_ebible_files, 1):
            print(f"{i:>4}   {previous_ebible_file.name}")
    else:
        print(f"All the expected eBible files are already downloaded.")

    if files_to_download:
        log_and_print(
            logfile,
            f"Downloading {len(files_to_download)} eBible files to {downloads_folder}.",
        )

        # Download the zip files.
        newly_downloaded_files = download_files(
            files_to_download,
            eBible_url,
            downloads_folder,
            logfile,
            redownload=args.force_download,
        )

        # Downloading complete.
        new_projects = unzip_files(
            zip_files=newly_downloaded_files,
            unzip_folder=projects_folder,
            file_suffix=file_suffix,
            logfile=logfile,
        )

        # Note how many settings files already exist
        existing_settings_file_count = len(
            [settings_file for settings_file in projects_folder.rglob("Settings.xml")]
        )

        # Add a Settings.xml file to each new project folder in a list of folders.
        settings_files = [write_settings_file(project) for project in new_projects]

        log_and_print(
            logfile,
            f"\nCreated {len(settings_files)} Settings.xml files. There were {existing_settings_file_count} projects with existing settings files.",
        )

        # Write the licence file.
        write_licence_file(licence_file, logfile, projects_folder)

        log_and_print(logfile, f"Wrote the licences.tsv file to the metadata folder.")

    else:
        log_and_print(logfile, f"All eBible files are already downloaded.")

    # Load-in the extracted licenses.tsv file
    licenses_df = pd.read_csv(licence_file, dtype=str)

    # Manually fix invalid rows:
    # https://ebible.org/Bible/details.php?id=engwmb
    # https://ebible.org/Bible/details.php?id=engwmbb

    licenses_df.loc[
        licenses_df["ID"].str.contains("engwmb"), "Copyright Holder"
    ] = "Public Domain"

    # Correctly set 'public domain' in License Type
    # pd.set_option('display.max_rows', 10)
    licenses_df.loc[
        licenses_df["Copyright Holder"].str.contains("Public") == True, "Licence Type"
    ] = "public"

    # Correctly set values for 'unknown' Licence Type
    licenses_df.loc[licenses_df["Licence Type"].isna(), "Licence Type"] = "unknown"

    # Show counts
    log_and_print(logfile, "These are the different licences.")
    log_and_print(logfile, licenses_df["Licence Type"].value_counts())

    # Check if the corpus includes any 'unknown' licensed projects

    # or if any known licensed projects have been excluded
    redistributable_filenames = sorted(
        set(
            licenses_df[~(licenses_df["Licence Type"].str.contains("unknown"))]["ID"]
            .apply(
                lambda x: f"{x[:3]}-{x}.txt"
                if "-" not in x
                else f'{x.split("-")[0]}-{x}.txt'
            )
            .to_list()
        )
    )
    log_and_print(
        logfile,
        f"There are {len(redistributable_files)} files with permissive licenses.",
    )
    
    for redistributable_filename in redistributable_filenames:
        source_file = private_corpus_folder / redistributable_filename
        dest_file = corpus_folder / redistributable_filename

        shutil.move(source_file, dest_file)
        # print(f"Moving {source_file} to {dest_file}")
    exit()

    current_corpus_files = set(
        [f"{corpus_file.name}" for corpus_file in corpus_folder.iterdir()]
    )
    log_and_print(
        logfile,
        f"Number of projects currently in the corpus = {len(current_corpus_files)}",
    )

    corpus_files_with_unknown_license = sorted(
        current_corpus_files - redistributable_files
    )
    # print(corpus_files_with_unknown_license)

    if corpus_files_with_unknown_license:
        log_and_print(
            logfile,
            f"\nThese {len(corpus_files_with_unknown_license)}  projects with an unknown license are currently in the corpus:",
        )
        for corpus_file_with_unknown_license in corpus_files_with_unknown_license:

            shutil.move(
                corpus_file_with_unknown_license, "path/to/new/destination/for/file.foo"
            )
            log_and_print(
                logfile,
                f"{corpus_file_with_unknown_license}",
            )
    else:
        log_and_print(
            logfile,
            f"\nAll {len(corpus_files_with_unknown_license)}  projects with an unknown license are currently in the corpus:",
        )

    # TO DO: Use silnlp.common.extract_corpora to extract all the project files.
    # If silnlp becomes pip installable then we can do that here with silnlp as a dependency.

    # log_and_print(
    #     logfile,
    #     f"\nUse this command to extract the non_redistributable files.",
    # )

    log_and_print(
        logfile,
        [
            f"\nUse this command to extract the all the projects to the private_corpus.",
            f"poetry run python -m silnlp.common.bulk_extract_corpora --input {projects_folder} --output {private_corpus_folder} --error-log {extract_log_file}",
        ],
    )


if __name__ == "__main__":
    main()


"""
From line 600 ff
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
        logfile, f"There are {len(dont_download)} files that usually fail to download."
    )
    if do_download:
        log_and_print(logfile, f"There are {len(do_download)} files still to download.")
    else:
        log_and_print(logfile, f"All zip files have already been downloaded.")"""


"""# Check whether any non_redistributable files are in the redistributable folder and list them.
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

"""
# print("Read config.yaml successful")
# print(f"data is {config}")
# print(dont_download)
# print(private)
# print(public)

# It's actually too soon to decide which files are redistributable and which are not
# We should unzip all first, then compare the translations_csv info with that
# scraped from the copr.htm files.

# Unzip all to the same folder. These aren't pushed to github.
# And we don't know the licence until it is read from the copr.html file.
# non_redistributable_files = sorted(set(all_files) - set(redistributable_files))

# print(f"Redist files = {redistributable_zipfiles}, {type(redistributable_zipfiles)}")
# print(f"Non redist files = {non_redistributable_zipfiles}, {type(non_redistributable_zipfiles)}")

# Unzip all to the same folder. These aren't pushed to github.
# And we don't know the licence until it is read from the copr.html file.
# print(f"{non_redistributable_files[0]} , {type(non_redistributable_files)}")
