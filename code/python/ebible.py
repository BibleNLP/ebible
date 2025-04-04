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
from dataclasses import dataclass
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


@dataclass
class Translation:
    language_code: str
    id: str
    is_redistributable: bool


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


def create_project_name(translation: Translation) -> str:
    """
    Creates a the destination project name for the translation passed by using a possibly modified version of the translation id.

    Examples:
           Translation id  ------>  Project name
           aoj                      aoj
           abt-maprik               maprik
           eng-web-c                web_c

    Note that in the last 2 examples:
    - the language code was removed in anticipation of the bulk_extract_corpora adding it later.
    - hyphens were converted to underscores

    For context, see discussion: https://github.com/BibleNLP/ebible/issues/55#issuecomment-2777490989
    If we _don't_ apply these transformations, the extract filename produced later by bulk_extract_corpora will be incorrect:

     Case    Project name     Extract filename     Correct?
     --------------------------------------------------------
     1       aoj              aoj-aoj.txt          Yes
     2       abt-maprik       abt-abt-maprik.txt   No, should be abt-maprik.txt
     3       eng-web-c        eng-eng-web-c.txt    No, should be eng-web_c.txt
                              ^^^^
                              ^^^^
                              prefix
                              added by bulk_extract_corpora

    Note that this transformation logic isn't applied to the files within the zip.
    See also settings_file.py `write_settings_file` and rename_usfm.py
    """
    if translation.id.startswith(translation.language_code + "-"):
        # Case 2 and 3
        return translation.id[len(translation.language_code) + 1:].replace("-", "_")
    else:
        # Case 1 - no transformation needed
        return translation.id


def get_translations(translations_csv: Path, only_redistributable: bool = True) -> List[Translation]:
    """
    Extracts the useful translation information from the translation.csv file.

    Note that only Bibles with at least 400 verses are included.

    The only_redistributable argument can be used to limit it to only extracting translations that are redistributable.
    This is useful for times when we are just wanting to regenerate the corpus uploaded to hugging face as it later avoids
    downloading a lot of zips from ebible that we don't actually use.
    """

    translations: List[Translation] = []

    with open(translations_csv, encoding="utf-8-sig", newline="") as csvfile:
        reader = DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            is_redistributable = row["Redistributable"]

            if is_redistributable or not only_redistributable:
                language_code: str = row["languageCode"]
                translation_id: str = row["translationId"]

                total_verses = int(row["OTverses"]) + int(row["NTverses"])

                if (total_verses >= 400):
                    translations.append(Translation(language_code, translation_id, is_redistributable))

        return translations


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
    parser.add_argument(
        "--only-redistributable",
        default=True,
        action="store_true",
        help="Controls whether we want to download and process only redistributable Bible translations",
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

    # Wipe all data in the projects_folder and private_projects_folder
    # to prevent stale changes from previous iterations
    for dir in [projects_folder, private_projects_folder]:
        projects = [f for f in dir.iterdir() if f.is_dir()]
        for project in projects:
            shutil.rmtree(project)
    # Wipe clean the corpus directory
    for corpus_file in corpus_folder.iterdir():
        corpus_file.unlink()

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

    translations = get_translations(translations_csv, args.only_redistributable)

    # This defines the set of translation id's that we will unpack into projects.
    translation_ids = [translation.id for translation in translations]

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

    translation_ids_to_download = (
        set(translation_ids)
        - set(existing_translation_ids)
        - set(dont_download_translation_ids)
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
    else:
        log_and_print(logfile, "All eBible files are already downloaded.")

    # Create the project directories for each translation
    # by unzipping them and creating a Settings.xml file within each
    for translation_id in translation_ids:
        log_and_print(logfile, f"Creating project for translation {translation_id}")
        translation = next(t for t in translations if t.id == translation_id)

        # Determine where to download to
        # NOTE: Previous versions of this script would first check if the translation had already been
        # unpacked to the public or private dir on a previous run.
        # The script now wipes these directories clean beforehand so such a check no longer makes sense.
        if translation.is_redistributable:
            unzip_dir = projects_folder
        else:
            unzip_dir = private_projects_folder
        project_dir = unzip_dir / create_project_name(translation)

        # Unzip it
        project_dir.mkdir(parents=True, exist_ok=True)
        log_and_print(logfile, f"Extracting translation {translation.id} to: {project_dir}")
        try:
            shutil.unpack_archive(build_download_path(translation.id), project_dir)
        except shutil.ReadError:
            log_and_print(logfile, f"ReadError: While trying to unzip: {project_dir}")
        except FileNotFoundError:
            log_and_print(
                logfile, f"FileNotFoundError: While trying to unzip: {project_dir}"
            )

        write_settings_file(project_dir, translation.language_code, translation.id)

        rename_usfm(project_dir)


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
