"""ebible.py contains functions for downloading and processing data from eBible.org.
The normal pipeline is to check whether a file exists before downloading it
It is only downloaded or unzipped if it doesn't already exist.
First download translations.csv from https://ebible.org/Scriptures/translations.csv
Read the list of available files from translations.csv
Download zipped Bibles in USFM format from ebible.org to the 'downloads' folder.
Unzip the downloaded files to the 'projects' folder.
Check the licence data contained in the copr.htm files in the redistributable_folder folders. Write that as a csv file.
Check the existence of verses in certain chapters in order to guess the versification. Add that to a Settings.xml in each project.
The Settings.xml file is necessary for silnlp.common.extract_corpora.
Move any private projects (complete with Settings.xml file) from the projects folder to the private_projects folder.
Print out the two commands necessary for extracting from the projects folder to the corpus folder,
and from the private_projects folder to the private_corpus folder.
The user then needs to use SILNLP https://github.com/sillsdev/silnlp to extract the files into the one-verse-per-line format.
"""

import argparse
import shutil
import os
from csv import DictReader
from datetime import datetime
from pathlib import Path
from random import randint
from time import sleep, strftime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import regex
import requests
import yaml
from bs4 import BeautifulSoup

from settings_file import write_settings_file
from rename_usfm import rename_usfm

global headers
headers: Dict[str, str] = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0",
}


# Define methods for downloading and unzipping eBibles
def log_and_print(file, messages, log_type="Info") -> None:

    if isinstance(messages, str):
        with open(file, "a") as log:
            log.write(
                f"{log_type}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {messages}\n"
            )
            print(messages)

    if isinstance(messages, list):
        with open(file, "a") as log:
            for message in messages:
                log.write(
                    f"{log_type}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}\n"
                )
                print(message)


def make_directories(dirs_to_create) -> None:
    for dir_to_create in dirs_to_create:
        dir_to_create.mkdir(parents=True, exist_ok=True)


def download_file(url, file, headers=headers) -> Optional[Path]:

    r = requests.get(url, headers=headers)
    # If the status is OK continue
    if r.status_code == requests.codes.ok:
        with open(file, "wb") as out_file:
            # Write out the content of the page.
            out_file.write(r.content)

        return file
    return None


def download_files(
    files_and_translation_ids: List[Tuple[Path, str]],
    base_url: str,
    folder: Path,
    logfile: Path,
    redownload: bool = False,
) -> List[Path]:

    downloaded_files = []

    for i, (file, translation_id) in enumerate(files_and_translation_ids):

        # Construct the download url and the local file path.
        url = f"{base_url}{translation_id}_usfm.zip"
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


def unzip_files(
    zip_files: List[Path],
    unzip_folder: Path,
    also_check: Path,
    file_suffix: str,
    logfile,
) -> List[Path]:

    # Keep track of which files were unzipped
    unzipped = []
    # Strip off the file_suffix so that the unzip folder name is the project ID.
    for zip_file in zip_files:
        project_foldername = zip_file.stem
        unzip_to_folder = unzip_folder / project_foldername
        also_check_folder = also_check / project_foldername
        if not unzip_to_folder.exists() and not also_check_folder.exists():
            # The file still needs to be unzipped.

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


def get_redistributable(translations_csv: Path) -> Tuple[List[str], List[str]]:

    redistributable_translation_ids: List = []
    all_translation_ids: List = []

    with open(translations_csv, encoding="utf-8-sig", newline="") as csvfile:
        reader = DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            translation_id: str = row["translationId"]
            all_translation_ids.append(translation_id)

            if row["Redistributable"] == "True":
                redistributable_translation_ids.append(translation_id)

        return all_translation_ids, redistributable_translation_ids



def get_licence_details(logfile, folder) -> List[Dict[str, object]]:
    """
    Extracts licence details from the unzipped folders inside the `folder` passed.
    It is assumed that the unzipped folders will contain a copr.htm file.
    The details are returned in a list of config dictionaries, sorted by the project name.
    """

    column_headers = [
        "ID",
        "File",
        "Language",
        "Dialect",
        "Vernacular Title",
        "Licence Type",
        "Licence Version",
        "CC Licence Link",
        "Copyright Holder",
        "Copyright Years",
        "Translation by",
    ]

    # Get copyright info from eBible projects

    data = list()

    log_and_print(
        logfile, f"\nCollecting eBible copyright information from projects in {folder}"
    )

    for i, project in enumerate(sorted(os.listdir(folder))):
        copyright_path = folder / project / "copr.htm"
        if not copyright_path.exists():
            log_and_print(f"Unable to find copr.htm file for project '{project}' at expected path: {copyright_path} - aborting")
            exit()

        entry = dict.fromkeys(column_headers)

        id = copyright_path.parents[0].name
        entry["ID"] = id
        # TODO - can we stringify this and so return a Dict[str, str]
        entry["File"] = copyright_path

        with open(copyright_path, "r", encoding="utf-8") as copr:
            html = copr.read()
            soup = BeautifulSoup(html, "lxml")

        cclink = soup.find(href=regex.compile("creativecommons"))
        if cclink:
            # TODO - find an example that uses this
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
                    cc_by_match = regex.match(
                        r".*?/licenses/by(?P<version>.*)/", ref
                    )
                    if cc_by_match:
                        # print(f'Licence version = {cc_by_match["version"]}')
                        entry["Licence Type"] = "by"
                        entry["Licence Version"] = cc_by_match["version"]

        titlelink = soup.find(href=regex.compile(f"https://ebible.org/{id}"))
        if titlelink:
            entry["Vernacular Title"] = titlelink.string

        copy_strings = [s for s in soup.body.p.stripped_strings]

        for j, copy_string in enumerate(copy_strings):
            if j == 0 and "copyright ©" in copy_string:
                entry["Copyright Years"] = copy_string
                entry["Copyright Holder"] = copy_strings[j + 1]
            if j > 0 and "Language:" in copy_string:
                entry["Language"] = copy_strings[j + 1]

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
                entry["Copyright Years"] = ""
                entry["Copyright Holder"] = "Public Domain"

        data.append(entry)

    return data


def write_licence_file(licence_file, logfile, df):

    # df.columns = [
    #     "ID",
    #     "File",
    #     "Language",
    #     "Dialect",
    #     "Vernacular Title",
    #     "Licence Type",
    #     "Licence Version",
    #     "CC Licence Link",
    #     "Copyright Holder",
    #     "Copyright Years",
    #     "Translation by",
    # ]

    df.to_csv(licence_file, sep="\t", index=False)

    log_and_print(
        logfile, f"Wrote licence info for {len(df)} translations to {licence_file}\n"
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
            f"The following {len(missing_folders)} folders are required and will be created if you continue."
        )
        for folder in missing_folders:
            print(folder)

        print(f"\n\nAre you sure this is the right folder:    {base} ")
        if choose_yes_no("Enter Y to continue or N to Quit."):

            # Create the required directories
            make_directories(missing_folders)
            log_and_print(logfile, f"All the required folders were created at {base}\n")
        else:
            exit()

    else:
        log_and_print(logfile, f"All the required folders exist in {base}")


def move_projects(
    projects_to_move: List, parent_source_folder: Path, parent_dest_folder: Path
) -> List[Path]:

    moved = []
    for project_to_move in projects_to_move:
        source_folder = parent_source_folder / project_to_move
        dest_folder = parent_dest_folder / project_to_move
        # print(
        #     f"{source_folder} exists: {source_folder.exists()}   Dest: {dest_folder} exists: {dest_folder.exists()}  Move: {source_folder.exists() and not dest_folder.exists()}"
        # )

        if source_folder.exists() and not dest_folder.exists():
            shutil.move(str(source_folder), str(dest_folder))
            assert not source_folder.exists()
            assert dest_folder.exist()
            moved.append(project_to_move)

    return moved


def is_dir(folder):
    if folder.is_dir():
        return folder
    return False


def main() -> None:

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Download, unzip and extract text corpora from eBible."
    )
    parser.add_argument(
        "-f",
        "--filter",
        default=None,
        help="Defines a regex filter to limit the translation id's downloaded.",
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
    parser.add_argument(
        "--try-download",
        default=False,
        action="store_true",
        help="Set this flag to try and download only the non-downloadable exceptions specified in the config.yaml file.",
    )
    parser.add_argument("folder", help="The base folder where others will be created.")

    args: argparse.Namespace = parser.parse_args()
    # print(args, type(args))
    # exit()

    # Define base folder
    base: Path = Path(args.folder)

    translations_csv_url: str = r"https://ebible.org/Scriptures/translations.csv"
    eBible_url: str = r"https://ebible.org/Scriptures/"
    file_suffix: str = ".zip"

    corpus_folder: Path = base / "corpus"
    downloads_folder: Path = base / "downloads"
    private_corpus_folder: Path = base / "private_corpus"
    private_projects_folder: Path = base / "private_projects"
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
    public_extract_log: Path = logs_folder / ("extract_public" + log_suffix)
    private_extract_log: Path = logs_folder / ("extract_private" + log_suffix)

    # The file we will save that contains the licence information for each file.
    licence_file: Path = metadata_folder / "licences.tsv"

    check_folders_exist(
        [
            corpus_folder,
            downloads_folder,
            private_corpus_folder,
            private_projects_folder,
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
            zip,
        )
        download_file(translations_csv_url, translations_csv)
    else:
        log_and_print(
            logfile, f"translations.csv file already exists in: {str(translations_csv)}"
        )

    # Get the exceptions from the config.yaml file.
    with open(Path(__file__).with_name("config.yaml"), "r") as yamlfile:
        config: Dict = yaml.safe_load(yamlfile)

    dont_download_translation_ids = config["No Download"]

    def build_download_path(translation_id: str) -> Path:
        return downloads_folder / (translation_id + file_suffix)

    def parse_translation_id(download_path: Path) -> str:
        return download_path.stem

    if args.try_download:
        print("Try to download the exceptions in the config.yaml file.")
        ebible_filenames_and_translation_ids = [
            (build_download_path(translation_id), translation_id)
            for translation_id in config["No Download"]
        ]

        # Download the zip files.
        downloaded_files = download_files(
            ebible_filenames_and_translation_ids,
            eBible_url,
            downloads_folder,
            logfile,
            redownload=args.force_download,
        )

        if downloaded_files:
            log_and_print(
                logfile,
                f"Downloaded {len(downloaded_files)} eBible files to {downloads_folder}.",
            )
        # Downloading complete.
        exit()

    # These files have fewer than 400 lines of text in January 2023
    dont_download_translation_ids.extend(config["Short"])

    if args.filter:
        translation_ids = [
            id
            for id in translation_ids
            if regex.match(args.filter, id)
        ]
        log_and_print(logfile, f"Command line filter used to reduce to translation id's to {','.join(translation_ids)}")

    # translation id's from the current list that already have corresponding files in the download directory
    existing_translation_ids: List[str] = [
        translation_id
        for translation_id in translation_ids
        if build_download_path(translation_id).is_file()
    ]

    # Represents translation id's downloaded on a previous run to the base directory,
    # but are no longer available on eBible.org
    removed_translation_ids: List[str] = [
        parse_translation_id(download_path)
        for download_path in downloads_folder.glob("*" + file_suffix)
        if parse_translation_id(download_path) not in translation_ids
    ]

    translation_ids_to_download = (
        set(translation_ids)
        - set(existing_translation_ids)
        - set(dont_download_translation_ids)
    )

    # Presumably any other files used to be in eBible but have been removed
    # Note these in the log file, but don't remove them.

    log_and_print(
        logfile,
        f"Of {len(translation_ids)} ebible translation id's, {len(existing_translation_ids)} are already downloaded and {len(dont_download_translation_ids)} are excluded.",
    )
    if len(removed_translation_ids) > 0:
        log_and_print(
            logfile,
            f"These {len(removed_translation_ids)} removed translation id's have files in the download folder, but are no longer listed in translations.csv:",
        )
        for i, removed_translation_id in enumerate(removed_translation_ids, 1):
            log_and_print(
                logfile, f"{i:>4}   {build_download_path(removed_translation_id).name}"
            )
    else:
        log_and_print(
            logfile,
            "All the files in the download folder are listed in the translations.csv file.",
        )

    # Download the zip files.
    files_and_translation_ids_to_download = [
        (build_download_path(translation_id), translation_id)
        for translation_id in translation_ids_to_download
    ]

    downloaded_files = download_files(
        files_and_translation_ids_to_download,
        eBible_url,
        downloads_folder,
        logfile,
        redownload=args.force_download,
    )

    if downloaded_files:
        log_and_print(
            logfile,
            f"Downloaded {len(downloaded_files)} eBible files to {downloads_folder}.",
        )
    # Downloading complete.

    else:
        log_and_print(logfile, "All eBible files are already downloaded.")

    # Unzip all the zipfiles in the download folder to the projects_folder
    # Unless they have already been unzipped to either the projects_folder or private projects_folder
    unzip_files(
        zip_files=[zipfile for zipfile in downloads_folder.glob("*" + file_suffix)],
        unzip_folder=projects_folder,
        also_check=private_projects_folder,
        file_suffix=file_suffix,
        logfile=logfile,
    )

    project_folders = [project_folder for project_folder in projects_folder.iterdir()]

    private_project_folders = [
        private_project_folder
        for private_project_folder in private_projects_folder.iterdir()
    ]

    for private_project_folder in private_project_folders:
        # Add a Settings.xml file if necessary
        write_settings_file(private_project_folder)

    for project_folder in project_folders:
        # Add a Settings.xml file if necessary
        write_settings_file(project_folder)

    # Get projects licence details
    data = get_licence_details(logfile, projects_folder)

    # Load the licenses data into a pandas dataframe
    # The schema comes from the columns defined in `get_licence_details`
    licenses_df = pd.DataFrame.from_records(data)

    # Fix invalid rows:

    # The translations engwmb and engwmbb have an error in the licence details
    # in the field "Copyright Holder".
    # It needs to be corrected to "Public Domain"
    licenses_df.loc[licenses_df["ID"].str.contains("engwmb"), "Copyright Holder"] = "Public Domain"

    # Some fields have "Public" for "Licence Type" but it should be "Public Domain"
    licenses_df.loc[
        licenses_df["Copyright Holder"].str.contains("Public") == True, "Licence Type"
    ] = "Public Domain"

    # Correctly set values for 'Unknown' Licence Type
    licenses_df.loc[licenses_df["Licence Type"].isna(), "Licence Type"] = "Unknown"

    # Write the licence file.
    write_licence_file(licence_file, logfile, licenses_df)

    # Show counts by licence type
    log_and_print(logfile, "These are the numbers of files with each type of licence:")
    log_and_print(logfile, f"{licenses_df['Licence Type'].value_counts()}")

    # Get lists of public and private projects from the licences
    # This is done by splitting the licence data into two groups:
    # - "Licence Type" is not "Unknown" (public)
    # - "Licence Type" is "Unknown" (private)
    # For each group, we slice out the "ID" column which represents the original translation id
    # Note that the public projects are also extended to include those in the config.yml under "Public"
    # Ditto for private projects and "Private" in config.yml
    public_projects_in_licence_file = [
        project_id
        for project_id in licenses_df[
            # Note the `~` below indicates "NOT"
            ~(licenses_df["Licence Type"].str.contains("Unknown"))
        ]["ID"]
    ]

    private_projects_in_licence_file = [
        project_id
        for project_id in licenses_df[
            licenses_df["Licence Type"].str.contains("Unknown")
        ]["ID"]
    ]
    public_projects = public_projects_in_licence_file.copy()
    public_projects.extend(config["Public"])

    # Move the private projects to the private_projects dir
    private_projects = private_projects_in_licence_file.copy()
    private_projects.extend(config["Private"])
    for private_project in private_projects:
        misplaced_private_project = projects_folder / private_project
        if misplaced_private_project.is_dir():
            dest = private_projects_folder / misplaced_private_project.name
            log_and_print(
                logfile,
                f"This project is not redistributable and will be moved to the private projects folder: {dest}",
            )
            shutil.move(str(misplaced_private_project), str(dest))

    rename_usfm(projects_folder)
    rename_usfm(private_projects_folder)

    # TO DO: Use silnlp.common.extract_corpora to extract all the project files.
    # If silnlp becomes pip installable then we can do that here with silnlp as a dependency.

    log_and_print(
        logfile,
        [
            "\nUse this command to extract the private_projects to the private_corpus.",
            f"poetry run python -m silnlp.common.bulk_extract_corpora --input {private_projects_folder} --output {private_corpus_folder} --error-log {private_extract_log}",
        ],
    )

    log_and_print(
        logfile,
        [
            "\nUse this command to extract the public_projects to the public_corpus.",
            f"poetry run python -m silnlp.common.bulk_extract_corpora --input {projects_folder} --output {corpus_folder} --error-log {public_extract_log}",
        ],
    )


if __name__ == "__main__":
    main()
