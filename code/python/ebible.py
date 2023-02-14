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

# Import modules and directory paths
import argparse

# import codecs
# import ntpath
import os

# import re
import shutil
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
        project_foldername = (
            f"{zip_file.name[0: (len(zip_file.name) - len(file_suffix))]}"
        )
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


def get_licence_details(logfile, folder) -> List:

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
    copr_regex = r".*[/\\](?P<id>.*?)[/\\]copr.htm"

    log_and_print(
        logfile, "Collecting eBible copyright information from projects in {folder}"
    )

    for i, copyright_file in enumerate(sorted(folder.glob("**/copr.htm"))):
        entry = dict.fromkeys(column_headers)
        entry["ID"] = str(copyright_file.parents[0].relative_to(folder))

        id_match = regex.match(copr_regex, str(copyright_file))

        if not id_match:
            print(f"Can't match {copr_regex} to str{copyright_file}.")
            exit()

        else:
            id = id_match["id"]

            if i % 250 == 0:
                print(f"Read {i} files. Now reading: {copyright_file} with ID: {id}")

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
                        cc_by_match = regex.match(
                            r".*?/licenses/by(?P<version>.*)/", ref
                        )
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


def move_projects(projects_to_move: List, parent_source_folder:Path, parent_dest_folder: Path) -> List[Path]:

    moved = []
    for project_to_move in projects_to_move:
        source_folder =  parent_source_folder / project_to_move
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
    file_suffix: str = "_usfm.zip"

    corpus_folder: Path = base / "corpus"
    downloads_folder: Path = base / "downloads"
    private_corpus_folder: Path = base / "private_corpus"
    private_projects_folder: Path = base / "private_projects"
    projects_folder: Path = base / "projects"
    metadata_folder: Path = base / "metadata"
    logs_folder: Path = base / "logs"

    # The csv file to download from eBible.org
    translations_csv: Path = metadata_folder / "translations.csv"
    settings_filename = "Settings.xml"

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

    dont_download_filenames = [
        project + "_usfm.zip" for project in config["No Download"]
    ]

    if args.try_download:
        print("Try to download the exceptions in the config.yaml file.")
        ebible_filenames = [ project + "_usfm.zip" for project in config["No Download"]]
        ebible_files = [downloads_folder / ebible_filename for ebible_filename in ebible_filenames ]

        # Download the zip files.
        downloaded_files = download_files(
        ebible_files,
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
    dont_download_filenames.extend([ project + "_usfm.zip" for project in config["Short"]])

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

    # Presumably any other files used to be in eBible but have been removed
    # Note these in the log file, but don't remove them.

    log_and_print(
        logfile,
        f"Of {len(ebible_files)} ebible files, {len(existing_ebible_files)} are already downloaded and {len(dont_download_files)} are excluded.",
    )
    if len(previous_ebible_files) > 0:
        log_and_print(
            logfile,
            f"These {len(previous_ebible_files)} files are already in the download folder that are no longer listed in translations.csv:",
        )
        for i, previous_ebible_file in enumerate(previous_ebible_files, 1):
            log_and_print(logfile, f"{i:>4}   {previous_ebible_file.name}")
    else:
        log_and_print(
            logfile,
            f"All the files in the download folder are listed in the translations.csv file.",
        )

    # Download the zip files.
    downloaded_files = download_files(
        files_to_download,
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
        log_and_print(logfile, f"All eBible files are already downloaded.")

    # Unzip all the zipfiles in the download folder to the projects_folder
    # Unless they have already been unzipped to either the projects_folder or private projects_folder
    new_projects = unzip_files(
        zip_files=[zipfile for zipfile in downloads_folder.glob("*" + file_suffix)],
        unzip_folder=projects_folder,
        also_check=private_projects_folder,
        file_suffix=file_suffix,
        logfile=logfile,
    )

    project_folders = [project_folder for project_folder in projects_folder.iterdir()]
    project_foldernames = [project_folder.name for project_folder in project_folders]

    private_project_folders = [
        private_project_folder
        for private_project_folder in private_projects_folder.iterdir()
    ]
    private_project_foldernames = [
        private_project_folder.name
        for private_project_folder in private_project_folders
    ]

    for private_project_folder in private_project_folders:
        # Add a Settings.xml file if necessary
        write_settings_file(private_project_folder)

    for project_folder in project_folders:
        # Add a Settings.xml file if necessary
        write_settings_file(project_folder)

    # Get projects licence details
    data = get_licence_details(logfile, projects_folder)

    # Get private_projects licence details
    data.extend(get_licence_details(logfile, private_projects_folder))

    #data_keys = set(k  for project in data for k,v in project.items())
    #print(data_keys)
    #exit()

    # Don't write and Load-in the extracted licenses.tsv file
    # Instead convert to DataFrame, fix up and write out.
    # licenses_df = pd.read_csv(licence_file, dtype=str)

    # Load the licenses data into a pandas dataframe
    licenses_df = pd.DataFrame.from_records(data)

    # Fix invalid rows:
    # https://ebible.org/Bible/details.php?id=engwmb
    # https://ebible.org/Bible/details.php?id=engwmbb

    licenses_df.loc[
        licenses_df["ID"].str.contains("engwmb"), "Copyright Holder"
    ] = "Public Domain"

    # Correctly set 'public domain' in License Type
    # pd.set_option('display.max_rows', 10)
    licenses_df.loc[
        licenses_df["Copyright Holder"].str.contains("Public") == True, "Licence Type"
    ] = "Public Domain"

    # Correctly set values for 'Unknown' Licence Type
    licenses_df.loc[licenses_df["Licence Type"].isna(), "Licence Type"] = "Unknown"

    # Write the licence file.
    write_licence_file(licence_file, logfile, licenses_df)

    # Show counts
    log_and_print(logfile, "These are the numbers of files with each type of licence:")
    log_and_print(logfile, f"{licenses_df['Licence Type'].value_counts()}")

    # Get lists of public and private projects from the licences
    public_projects_in_licence_file = [project_id for project_id in 
        licenses_df[~(licenses_df["Licence Type"].str.contains("Unknown"))]["ID"]
    ]

    private_projects_in_licence_file = [project_id for project_id in licenses_df[
        licenses_df["Licence Type"].str.contains("Unknown")
    ]["ID"]]

    # Move any redistributable projects for the private_projects to the public_projects folder.
    moved_public_projects = move_projects(public_projects_in_licence_file, parent_source_folder = private_projects_folder, parent_dest_folder = project_folder)
    for moved_public_project in moved_public_projects:
        log_and_print(logfile, f"Moved redistributable project {moved_public_project} to {project_folder}")
    
    # Move the non-redistributable projects to the private_projects folder.
    moved_private_projects = move_projects(private_projects_in_licence_file, parent_source_folder = project_folder,  parent_dest_folder = private_projects_folder)
    for moved_private_project in moved_private_projects:
        log_and_print(logfile, f"Moved redistributable project {moved_private_project} to {private_project_folder}")
    

    # for private_project_in_licence_file in private_projects_in_licence_file:
    #     source_folder = projects_folder / private_project_in_licence_file
    #     dest_folder = private_projects_folder / private_project_in_licence_file
    #     print(
    #         f"{source_folder} exists: {source_folder.exists()}   Dest: {dest_folder} exists: {dest_folder.exists()}  Move: {source_folder.exists() and not dest_folder.exists()}"
    #     )
        
    #     if source_folder.exists() and not dest_folder.exists():
    #        print(f"Moving {source_folder} to {dest_folder}.")
    #        shutil.move(str(source_folder), str(dest_folder))
    #        assert not source_folder.exists()
    #        assert dest_folder.exist()


    # TO DO: Use silnlp.common.extract_corpora to extract all the project files.
    # If silnlp becomes pip installable then we can do that here with silnlp as a dependency.

    log_and_print(
        logfile,
        [
            f"\nUse this command to extract the private_projects to the private_corpus.",
            f"poetry run python -m silnlp.common.bulk_extract_corpora --input {private_projects_folder} --output {private_corpus_folder} --error-log {private_extract_log}",
        ],
    )

    log_and_print(
        logfile,
        [
            f"\nUse this command to extract the public_projects to the public_corpus.",
            f"poetry run python -m silnlp.common.bulk_extract_corpora --input {projects_folder} --output {corpus_folder} --error-log {public_extract_log}",
        ],
    )


if __name__ == "__main__":
    main()
