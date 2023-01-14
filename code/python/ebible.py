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
from datetime import date, datetime
from glob import iglob
from os import listdir
from pathlib import Path
from random import randint
from time import sleep, strftime

import pandas as pd
import regex
import requests
from bs4 import BeautifulSoup
from pandas.core.groupby import groupby

global headers
headers = {
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0",
}

# Define methods for downloading and unzipping eBibles


def log_and_print(file, s, type="Info"):

    with open(file, "a") as log:
        log.write(
            f"{type.upper()}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {s}\n"
        )
    print(s)


def make_directories(dirs_to_create):
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


def download_files(filenames, url, folder, logfile, redownload=False):

    for i, filename in enumerate(filenames):

        # Construct the download url and the local file path.
        url = url + filename
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
                    log_and_print(logfile, f"Saved {url} as {file}\n")
                    # Pause for a random number of miliseconds
                    pause = randint(1, 5000) / 1000
                    sleep(pause)
            continue

        else:
            log_and_print(logfile, f"{i+1}: Downloading from {url} to {file}.")
            done = download_file(url, file)

            if done:
                log_and_print(logfile, f"Saved {url} as {file}\n")
                # Pause for a random number of miliseconds
                pause = randint(1, 5000) / 1000
                sleep(pause)

            else:
                log_and_print(logfile, f"Could not download {url}\n")

    #log_and_print(logfile, f"Finished downloading eBible files\n")


def get_tree_size(path):
    """Return total size of files in given path and subdirs."""
    total = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            total += get_tree_size(entry.path)
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total


def unzip_ebible(source_file, dest_folder):

    if dest_folder.is_dir():
        log_and_print(logfile, f"Extracting {source_file} to: {dest_folder}")
        shutil.unpack_archive(source_file, dest_folder)
        # log_and_print(f"Extracted {source_file} to: {dest_folder}")

    else:
        log_and_print(logfile, f"Can't extract, {dest_folder} doesn't exist.")


def unzip_entire_folder(source_folder, file_suffix, unzip_folder, logfile):
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
            unzip_folder / f"{zip_file.name[0: (len(zip_file.name) - len(file_suffix))]}",
        )
        for zip_file in zip_files
    ]
    extracts = [
        (zip_file, folder)
        for zip_file, folder in extract_folders
        if not folder.exists() or zip_file.stat().st_size >= get_tree_size(folder)
    ]

    log_and_print(
        logfile,
        f"Found {len(extracts)} that were not yet extracted or are smaller than the zip file.\n",
    )

    for zip_file, extract in extracts:
        extract.mkdir(parents=True, exist_ok=True)
        log_and_print(logfile, f"Extracting to: {extract}")
        shutil.unpack_archive(zip_file, extract)

    #log_and_print(logfile, f"Finished unzipping eBible files\n")

    return len(extracts)

def unzip_files(zip_files, file_suffix, unzip_folder, logfile):

    log_and_print(logfile, f"\nUnzipping files to {unzip_folder}")
    
    # Strip off the file_suffix so that the subfolder name is the project ID.
    extract_folders = [
        (
            zip_file,
            unzip_folder / f"{zip_file.name[0: (len(zip_file.name) - len(file_suffix))]}",
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
        f"Found {len(extracts)} that were not yet extracted.",
    )

    for zip_file, extract in extracts:
        extract.mkdir(parents=True, exist_ok=True)
        log_and_print(logfile, f"Extracting to: {extract}")
        try:
            shutil.unpack_archive(zip_file, extract)
        except shutil.ReadError:
            log_and_print(logfile, f"ReadError: While trying to extract: {zip_file}")
        except FileNotFoundError:
            log_and_print(logfile, f"FileNotFoundError: While trying to extract: {zip_file}")

    #log_and_print(logfile, f"Finished unzipping eBible files\n")

    return len(extracts)


def get_redistributable(translations_csv):

    redistributable_files = []
    all_files = []

    with open(translations_csv, encoding="utf-8-sig", newline="") as csvfile:
        reader = DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            all_files.append(row["translationId"])

            if row["Redistributable"] == "True":
                redistributable_files.append(row["translationId"])

        return all_files, redistributable_files


# Define methods for creating the copyrights file


def norm_name(name):
    if len(name) < 3:
        return None
    elif len(name) == 3:
        return f"{name[:3]}-{name}"
    elif len(name) >= 7 and name[3] == "-" and name[0:3] == name[4:7]:
        return name
    else:
        return f"{name[:3]}-{name}"


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def read_list_from_file(f_in):
    lines = list()
    with open(f_in, "r", encoding="utf-8") as infile:
        for line in infile.read().splitlines():
            lines.append(line)
    return lines


def get_copyright_from_url(url):
    r = requests.get(url)
    # If the status is OK continue
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.content, "lxml")
        cr_item = soup.find("td", colspan="3")
        return cr_item.string
    else:
        return None


def get_download_lists(translations_csv, file_suffix, download_folder, wont_download):

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
def improve_column_names(df):
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace('"', "")
        .str.replace("'", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace(" ", "_")
    )


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


def get_conclusion(versification):

    if versification != "":
        return versification
    else:
        return "4"  # English


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
            print(f"Could not open {book_file}, reason:  {e}")
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
            print(f"Something went wrong in reading {book_file}, reason:  {e}")
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

    return versification


def add_settings_file(project_folder, language_code):
    versification = get_conclusion(get_versification(project_folder))
    setting_file_stub = f"""<ScriptureText>
    <Versification>{versification}</Versification>
    <LanguageIsoCode>{language_code}:::</LanguageIsoCode>
    <Naming BookNameForm="41-MAT" PostPart="{project_folder.name}.usfm" PrePart="" />
</ScriptureText>"""

    settings_file = project_folder / "Settings.xml"
    with open(settings_file, "w") as settings:
        settings.write(setting_file_stub)


def copy_to_working_directory(project, language_code, ebible_redistributable, rewrite):
    folder = ebible_redistributable / project.name
    if exists(folder):
        if rewrite:
            shutil.rmtree(folder)
        else:
            return 0
    log_and_print(f"copying {project.name} to {ebible_redistributable}")
    shutil.copytree(project, folder)
    add_settings_file(folder, language_code)
    return 1


def get_redistributable_projects(translations_csv, licences_csv):

    ok_copyrights = ["by-nc-nd", "by-nd", "by-sa"]
    redistributable = {}
    translations_info = pd.read_csv(translations_csv)
    licence_info = pd.read_csv(licences_csv)
    improve_column_names(translations_info)
    improve_column_names(licence_info)
    licence_info.rename(columns={"id": "translationid"}, inplace=True)
    print(f"licence info is\n{licence_info}")
    # print(f"licence info columns are\n{licence_info.columns}")

    combined = pd.merge(translations_info, licence_info, on="translationid", how="left")
    # print(f"Combined is\n{combined}")

    for index, row in combined.iterrows():
        if row["redistributable"] and (
            row["licence_type"] in ok_copyrights
            or row["copyright_holder"] == "Public Domain"
        ):
            redistributable[row["translationid"]] = row["languagecode"]

    return redistributable


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

    for i, copyright_file in enumerate(sorted(redistributable_folder.glob("**/copr.htm"))):
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


def main():
    parser = argparse.ArgumentParser(
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

    args = parser.parse_args()
    # print(args)
    # exit()

    # Define base folder
    base = Path(args.folder)

    translations_csv_url = r"https://ebible.org/Scriptures/translations.csv"
    eBible_url = r"https://ebible.org/Scriptures/"
    file_suffix = "_usfm.zip"

    # The corpus folder is for the verse aligned text files.
    corpus_folder = base / "corpus"

    # The redistributable_folder folder is for the redistributable_folder downloaded files.
    # To this folder we add a settings.xml file that we create.
    # The settings.xml file is required for the extraction process.
    redistributable_folder = base / "projects"

    # Folder for the non-redistributable projects.
    non_redistributable_folder = base / "private_projects"

    # The zipped folder is where we download files from eBible.org
    downloads_folder = base / "downloads"

    metadata_folder = base / "metadata"

    logs = base / "logs"
    
    year, month, day, hour, minute = map(int, strftime("%Y %m %d %H %M").split())
    log_suffix = f"_{year}_{month}_{day}-{hour}_{minute}.log"
    log_filename = ("ebible" + log_suffix)
    logfile = logs / log_filename

    extract_log_file = logs / ("extract" + log_suffix)

    # The file to download from eBible.org
    translations_csv = metadata_folder / "translations.csv"

    # The file we will save that contains the licence information for each file.
    licence_file = metadata_folder / "licences.tsv"

    all_folders = [corpus_folder, redistributable_folder, non_redistributable_folder, downloads_folder, metadata_folder, logs]
    missing_folders = [folder for folder in all_folders if not folder.is_dir()]

    print(f"The base folder is : {base}")
    # base = r"F:/GitHub/davidbaines/eBible"

    if missing_folders:
        log_and_print(logfile, "The following folders are required:")
        for folder in all_folders:
            log_and_print(logfile, folder)
        log_and_print(logfile, "")

        log_and_print(
            logfile,
            "The following folders are required and will be created if you continue.",
        )
        for folder in missing_folders:
            log_and_print(logfile, folder)

        choice = ""
        while choice not in ["y", "n"]:
            choice = input("Would you like to continue? y /n ").lower()
        if choice == "y":
            # Create the required directories
            make_directories(missing_folders)
        elif choice == "n":
            exit(0)
    else:
        log_and_print(logfile, "All required folders already exist\n")

    if not translations_csv.is_file() or args.force_download:
        # Download the list of translations.
        log_and_print(
            logfile,
            f"Downloading list of translations from {translations_csv_url} to: {str(translations_csv)}",
        )
        download_file(translations_csv_url, translations_csv)

    # These wont download usually.
    wont_download = [
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

    (
        all_files,
        redistributable_files,
        non_redistributable_files,
        to_download,
        already_downloaded,
    ) = get_download_lists(
        translations_csv, file_suffix, downloads_folder, wont_download=wont_download
    )

    redistributable_zipfiles = sorted([downloads_folder / (file + file_suffix) for file in redistributable_files])

    non_redistributable_zipfiles = sorted([downloads_folder / (file + file_suffix) for file in non_redistributable_files])

    #print(f"The first redistributable file is {redistributable_files[0]}") 
    #print(f"The first non redistributable file is {non_redistributable_files[0]}") 
    
    log_and_print(
        logfile,
        f"The translations csv file lists {len(all_files)} translations and {len(redistributable_files)} are redistributable.",
    )
    log_and_print(
        logfile,
        f"The translations csv file lists {len(non_redistributable_files)} non redistributable translations.",
    )
    log_and_print(
        logfile,
        f"{len(non_redistributable_files)} + {len(redistributable_files)} = {len(non_redistributable_files) + len(redistributable_files)}",
    )
    log_and_print(
        logfile,
        f"There are {len(already_downloaded)} files with the suffix {file_suffix} already in {downloads_folder}",
    )
    log_and_print(
        logfile, f"There are {len(wont_download)} files that usually fail to download."
    )
    log_and_print(logfile, f"There are {len(to_download)} files still to download.")

    # Download the required zipped USFM files.
    download_files(
        to_download, eBible_url, downloads_folder, logfile, redownload=args.force_download
    )

    #print(f"Redist files = {redistributable_zipfiles}, {type(redistributable_zipfiles)}")
    #print(f"Non redist files = {non_redistributable_zipfiles}, {type(non_redistributable_zipfiles)}")
    
    # Unzip the redistributable downloaded files.
    redistributable_project_count = unzip_files(redistributable_zipfiles, file_suffix, redistributable_folder, logfile)

    # Unzip the non_redistributable downloaded files to the private_projects folder.
    unzip_files(non_redistributable_zipfiles, file_suffix, non_redistributable_folder, logfile)

    

    #print(f"{non_redistributable_files[0]} , {type(non_redistributable_files)}")

    # Check whether any non_redistributable files are in the redistributable folder and list them.
    incorrect_non_redistributable_folders = [redistributable_folder / non_redistributable_file for non_redistributable_file in non_redistributable_files]
    incorrect_non_redistributable_folders = [folder for folder in incorrect_non_redistributable_folders if folder.is_dir()]
    if incorrect_non_redistributable_folders:
        print(f"There are {len(incorrect_non_redistributable_folders)} non_redistributable projects in the redistributable project folder: {redistributable_folder}")
        delete = input("Delete them Yes / No / Quit?")
        while  delete.strip()[0].lower() not in "ynq":
            print("Please enter either yes or no, or q to exit.")
            delete = input("Delete them Yes / No / Quit?")
        
        if delete.strip()[0].lower() == 'y':
            for incorrect_non_redistributable_folder in incorrect_non_redistributable_folders:
                shutil.rmtree(incorrect_non_redistributable_folder, ignore_errors=True)

        elif delete.strip()[0].lower() == 'q':
            exit()
            

    if redistributable_project_count:
        # Write the licence file.
        write_licence_file(licence_file, logfile, redistributable_folder)
    else:
        log_and_print(logfile, f"There are no changes required to the licence file.")


    # Now add the Settings.xml file to each project folder.
    # print(f"Files extracted. ")
    redistributable_projects = {
        folder: str(folder.name)[:3] for folder in redistributable_folder.glob("*") if folder.is_dir() and folder.name in redistributable_files
    }
    #print(redistributable_projects, len(redistributable_projects))
    
    count_existing_settings_files = 0
    count_new_settings_files = 0

    for project_folder, language_code in redistributable_projects.items():
        settings_file = project_folder / "Settings.xml"
        if settings_file.is_file():
            count_existing_settings_files += 1
            #print(f"Settings.xml already exists in {project_folder}")
        else:
            count_new_settings_files += 1
            #print(f"Adding Settings.xml to {project_folder}")
            add_settings_file(project_folder, language_code)

    print(f"\nCreated {count_new_settings_files} Settings.xml files. There are {count_existing_settings_files} existing settings files.")

    # TO DO: Use silnlp.common.extract_corpora to extract all the project files.
    # If silnlp becomes pip installable then we can do that here with silnlp as a dependency.
    
    print("\nUse this command ONLY if you want to extract the non_redistributable files.")
    print(f"poetry run python -m silnlp.common.bulk_extract_corpora --input {non_redistributable_folder} --output <OUTPUT_FOLDER>")


    print("\nThe files are ready for extracting. Use this command in the SILNLP repo to extract the redistributable files.")
    print(f"poetry run python -m silnlp.common.bulk_extract_corpora --input {redistributable_folder} --output {corpus_folder} --error-log {extract_log_file}")


if __name__ == "__main__":
    main()

    # Get list of redistributable files from the translations.csv and licence_file
    # Fix this, the combined file has only the project name in the translationid column.
    # The licence file has "languagecode-translationid" in the translationid column. So none match on translationid.
    # redistributable = get_redistributable_projects(translations_csv,licence_file)

    # extracted_files = [entry['ID'][4:] for entry in redistributable]
    # missing_files = [file + file_suffix for file in sorted(set(all_files) - set(extracted_files))]
    # unexpectedly_missing = set(missing_files) - set(wont_download)

    # print(f"There are {len(unexpectedly_missing)} missing extract folders.")

    # if len(unexpectedly_missing) == 0:
    #    print(f"There are exactly {len(wont_download)} missing redistributable_folder folders. They are the same {len(missing_files)} that we didn't try to download.")
    # else :
    #    print(f"{len(unexpectedly_missing)} files were not extracted. Upto 50 are shown:")
    #    for file in sorted(unexpectedly_missing)[:50]:
    #        print(file)
