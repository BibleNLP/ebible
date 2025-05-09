"""ebible.py contains functions for downloading and processing data from eBible.org.
The normal pipeline is to check whether a recent file exists before downloading it
It is only downloaded if it doesn't already exist.
First download translations.csv from https://ebible.org/Scriptures/translations.csv
Read the list of available files from translations.csv
Download zipped Bibles in USFM format from ebible.org to the 'downloads' folder.
Unzip the downloaded files to the 'projects' or 'private_projects' folder.
Check the licence data contained in the copr.htm files. Write that as a csv file.
Create a Settings.xml file for silnlp.common.bulk_extract_corpora.
Print out the two commands necessary for extracting from the public projects folder to the corpus folder,
and from the private_projects folder to the private_corpus folder.
The user then needs to use SILNLP https://github.com/sillsdev/silnlp bulk_extract_corpora to generate extract files.
"""

import argparse
import shutil
import os
from csv import DictReader
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from random import randint
from time import sleep, strftime
from typing import Dict, List, Optional

import pandas as pd
import regex
import requests
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


def find_recent_download(downloads_folder: Path, translation_id: str, max_zip_age_days: int) -> Optional[Path]:
    """
    Returns the local path of a recently downloaded zip file for this translation id (if one exists).

    Zip files are in the form: downloads/{translation_id}--YYYY-MM-DD.zip where the date is UTC.

    Zip files are considered "recent" if the number of days between now and when it was downloaded
    is within max_zip_age_days days
    """
    potential_zips: List[Path] = list(downloads_folder.glob(f"{translation_id}--*-*-*.zip"))

    if not potential_zips:
        return None

    def parse_date(zip: Path) -> date:
        result = regex.match(".*--(\\d{4})-(\\d{2})-(\\d{2})", zip.stem)
        return date(int(result.group(1)), int(result.group(2)), int(result.group(3)))

    zip_dates_sorted: List[date] = sorted([
        parse_date(zip)
        for zip in potential_zips
    ])

    most_recent = zip_dates_sorted[-1]

    now: date = datetime.now(timezone.utc).date()

    day_delta: int = (now - most_recent).days

    if day_delta <= max_zip_age_days:
        return downloads_folder / build_zip_filename(translation_id, most_recent)
    else:
        return None


def build_zip_filename(translation_id: str, date: date) -> str:
    return f"{translation_id}--{str(date)}.zip"


def download_files(
    translation_ids: List[str],
    base_url: str,
    folder: Path,
    logfile: Path,
) -> List[Path]:
    """
    Attempts to downloads zip files for the translation id's passed.
    The local file paths of the successfully downloaded zips are returned.
    """
    log_and_print(
        logfile,
        f"Attempting to download zips for {len(translation_ids)} translation id's",
    )
    downloaded_files: List[(str, Path)] = []

    for i, translation_id in enumerate(translation_ids):

        # Construct the download url and the local file path.
        url = f"{base_url}{translation_id}_usfm.zip"
        file = folder / build_zip_filename(translation_id, datetime.now(timezone.utc).date())

        log_and_print(logfile, f"{i+1}: Downloading from {url} to {file}.")
        if downloaded_file := download_file(url, file):
            downloaded_files.append((translation_id, downloaded_file))

            log_and_print(logfile, f"Saved {url} as {file}\n")

            # Pause for a random number of miliseconds
            sleep(randint(1, 5000) / 1000)

        else:
            log_and_print(logfile, f"Could not download {url}\n")

    log_and_print(
        logfile,
        f"\nFinished downloading. Downloaded {len(downloaded_files)}/{len(translation_ids)} zip files from eBbile.org",
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
           fra_fob                  fob

    Note that in the last 3 examples:
    - the language code prefix was removed in anticipation of the bulk_extract_corpora adding it later.
    - hyphens in the remaining translation description were converted to underscores

    For context, see discussion: https://github.com/BibleNLP/ebible/issues/55#issuecomment-2777490989
    If we _don't_ apply these transformations, the extract filename produced later by bulk_extract_corpora will be incorrect:

     Case    Project name     Extract filename     Correct?
     --------------------------------------------------------
     1       aoj              aoj-aoj.txt          Yes
     2       abt-maprik       abt-abt-maprik.txt   No, should be abt-maprik.txt
     3       eng-web-c        eng-eng-web-c.txt    No, should be eng-web_c.txt
     4       fra_fob          fra-fra_fob.txt      No, should be fra-fob.txt
                              ^^^^
                              ^^^^
                              prefix
                              added by bulk_extract_corpora

    Note that this transformation logic isn't applied to the files within the zip.
    See also settings_file.py `write_settings_file` and rename_usfm.py
    """
    if regex.match(f"^{regex.escape(translation.language_code)}[_\\-]", translation.id):
        # Cases 2-4 - the translation id begins with the language code plus an underscore or hyphen
        # The matched characters are removed, and remaining hyphens are replaced with underscores
        return translation.id[len(translation.language_code) + 1:].replace("-", "_")
    else:
        # Case 1 - no transformation needed
        return translation.id


def get_translations(translations_csv: Path, allow_non_redistributable: bool) -> List[Translation]:
    """
    Extracts the useful translation information from the translation.csv file.

    Translations are only included if:
    - they are downloadable (indicated by the "downloadable" column)
    - they have at least 400 verses (indicated by summing values from the "OTverses" and "NTverses" columns)
    - they are redistributable - if only_redistributable above is set (indicated by the "Redistributable" column)

    The only_redistributable is useful for times when we are just wanting to regenerate the corpus uploaded to hugging face
    and don't care about private translations.
    This avoids downloading a lot of zips from ebible that we don't actually use.
    """

    def parse_bool(s: str) -> bool:
        """Intended for boolean columns in the translation.csv that use either 'True' or 'False'"""
        return s.lower() == "true"

    translations: List[Translation] = []

    with open(translations_csv, encoding="utf-8-sig", newline="") as csvfile:
        reader = DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            is_redistributable = parse_bool(row["Redistributable"])

            if parse_bool(row["downloadable"]) and (is_redistributable or allow_non_redistributable):
                language_code: str = row["languageCode"]
                translation_id: str = row["translationId"]

                total_verses = int(row["OTverses"]) + int(row["NTverses"])

                if (total_verses >= 400):
                    translations.append(Translation(language_code, translation_id, is_redistributable))

        return translations


def get_licence_details(logfile, folder, project_path_to_translation_id: Dict[Path, str]) -> List[Dict[str, object]]:
    """
    Extracts licence details from the unzipped folders inside the `folder` passed.
    It is assumed that the unzipped folders will contain a copr.htm file.
    The details are returned in a list of config dictionaries, sorted by the project name.
    """

    column_headers = [
        "ID", # Original translation id corresponding to the project
        "File", # Path to the paratext project
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

        project_path = copyright_path.parents[0]
        id = project_path_to_translation_id[project_path]

        entry["ID"] = id
        # TODO - can we stringify this and so return a Dict[str, str]
        entry["File"] = copyright_path

        with open(copyright_path, "r", encoding="utf-8") as copr:
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

        titlelink = soup.find(href=regex.compile(f"https://ebible.org/{id}"))
        if titlelink:
            entry["Vernacular Title"] = titlelink.string

        if id in ["engwmb", "engwmbb"]:
            # The copr.htm for these two Bibles is structured differently,
            # where the useful information is spread across many paragraphs
            # in contrast to the other Bibles where the useful information is contained
            # completely within the first paragraph
            paragraphs = soup.find('body').find_all('p')
            copy_strings = [p.get_text(strip=True) for p in paragraphs]
        else:
            copy_strings = list(soup.body.p.stripped_strings)


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

            # TODO - improve logic to match cases like eng-lxx2012 where the search term
            # is separated from the translators such that they end up different stripped strings
            translation_search_term = "Translation by: "
            if translation_search_term in copy_string:
                entry["Translation by"] = copy_string[len(translation_search_term):]
            if "Public Domain" in copy_string:
                entry["Copyright Years"] = ""
                entry["Copyright Holder"] = "Public Domain"
                entry["Licence Type"] = "Public Domain"

        # If the licence type wasn't explicitly set in the steps above, set it to "Unknown"
        if not entry["Licence Type"]:
            entry["Licence Type"] = "Unknown"

        data.append(entry)

    return data


def write_licence_file(licence_file, logfile, df):

    # For a description of the schema, see `column_headers` list in method `get_licence_details`

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
        "--allow_non_redistributable",
        default=False,
        action="store_true",
        help="When true, allows non-redistributable (private) translations to be downloaded and processed",
    )
    parser.add_argument(
        "--download_only",
        default=False,
        action="store_true",
        help="When true, this causes the script to abort after the initial download of zip files",
    )
    parser.add_argument(
        "--max_zip_age_days",
        default=14,
        type=int,
        help="Sets the maximum age in days that a downloaded zip can have before it's considered too old and needs redownloading",
    )
    parser.add_argument("folder", help="The base folder where others will be created.")

    args: argparse.Namespace = parser.parse_args()
    # print(args, type(args))
    # exit()

    # Define base folder
    base: Path = Path(args.folder)

    translations_csv_url: str = r"https://ebible.org/Scriptures/translations.csv"
    eBible_url: str = r"https://ebible.org/Scriptures/"

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

    translations = get_translations(translations_csv, args.allow_non_redistributable)

    # This defines the set of translation id's that we will unpack into projects.
    translation_ids = [translation.id for translation in translations]

    if args.filter:
        translation_ids = [
            id
            for id in translation_ids
            if regex.match(args.filter, id)
        ]
        log_and_print(logfile, f"Command line filter used to reduce translation id's to {translation_ids}")

    log_and_print(logfile, f"{len(translation_ids)} translation id's will be processed")

    if args.force_download:
        # Download everything ignoring any caching
        translation_ids_to_download = translation_ids
    else:
        # Don't redownload zips that already have a recently cached version
        existing_translation_ids: List[str] = [
            translation_id
            for translation_id in translation_ids
            if find_recent_download(downloads_folder, translation_id, args.max_zip_age_days)
        ]
        translation_ids_to_download = list(set(translation_ids) - set(existing_translation_ids))

    downloaded_files = download_files(
        translation_ids_to_download,
        eBible_url,
        downloads_folder,
        logfile,
    )

    if downloaded_files:
        log_and_print(
            logfile,
            f"Downloaded {len(downloaded_files)} eBible files to {downloads_folder}.",
        )
    else:
        log_and_print(logfile, "No files were downloaded - either they were already downloaded or failed to download")

    if args.download_only:
        log_and_print(logfile, "Terminating as --download-only flag set")
        exit()


    # This mapping is needed later for setting the "ID" field in the licence details for each project
    project_path_to_translation_id: Dict[Path, str] = dict()

    # Create the project directories for each translation
    # by unzipping them and creating a Settings.xml file within each
    for translation_id in translation_ids:
        log_and_print(logfile, f"Creating project for translation {translation_id}")
        translation = next(t for t in translations if t.id == translation_id)

        # Determine where to unzip to
        # NOTE: Previous versions of this script would first check if the translation had already been
        # unpacked to the public or private dir on a previous run.
        # The script now wipes these directories clean beforehand so such a check no longer makes sense.
        if translation.is_redistributable:
            unzip_dir = projects_folder
        else:
            unzip_dir = private_projects_folder
        project_dir = unzip_dir / create_project_name(translation)

        project_path_to_translation_id[project_dir] = translation_id

        # Unzip it
        project_dir.mkdir(parents=True, exist_ok=True)
        log_and_print(logfile, f"Extracting translation {translation.id} to: {project_dir}")
        try:
            shutil.unpack_archive(find_recent_download(downloads_folder, translation.id, args.max_zip_age_days), project_dir)
        except shutil.ReadError:
            log_and_print(logfile, f"ReadError: While trying to unzip: {project_dir}")
        except FileNotFoundError:
            log_and_print(
                logfile, f"FileNotFoundError: While trying to unzip: {project_dir}"
            )

        write_settings_file(project_dir, translation.language_code, translation.id)

        rename_usfm(project_dir)


    # Get projects licence details
    data = get_licence_details(logfile, projects_folder, project_path_to_translation_id)

    # Load the licenses data into a pandas dataframe
    # The schema comes from the columns defined in `get_licence_details`
    licenses_df = pd.DataFrame.from_records(data)

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
