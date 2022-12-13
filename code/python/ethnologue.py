from typing import Tuple, List
from pathlib import Path
import json
import re
import time
from io import StringIO
import requests
from lxml import etree

ethnologue_folder = Path('..', '..', 'metadata')
ethnologue_file = Path(ethnologue_folder, 'languages.tsv')
exceptions_file = Path(ethnologue_folder, 'exceptions.tsv')
languages = []


def addLanguage(iso: str, langName: str, langFam: str, langCountry: str):
    languages.append({
                        'isoCode': iso,
                        'language': langName,
                        'languageFamily': langFam,
                        'langCountry': langCountry,
                     })


def writeLanguages(outFile: Path, langs: List):
    with open(outFile, 'w', encoding='utf-8') as f:
        for lang in langs:
            f.write(f'{lang["isoCode"]}\t{lang["language"]}\t{lang["languageFamily"]}\t{lang["langCountry"]}\n')


def loadLanguages(inFile: Path) -> List:
    langs = []
    try:
        with open(inFile, 'r', encoding='utf-8') as f:
            for line in f:
                tokens = line.rstrip().split('\t')
                langs.append({
                    'isoCode': tokens[0],
                    'language': tokens[1],
                    'languageFamily': tokens[2],
                    'langCountry': tokens[3],
                })
    except Exception as e:
        print(f'ERROR: Unable to load languages from ({inFile}), error: {e}')
        return []

    return langs


def setLanguageFamilyLocation(file: Path = Path('languageFamilies.json')):
    global ethnologue_file

    if Path.isfile(file):
        ethnologue_file = file
    else:
        print(f'WARNING: {file} does not exist, defaulting to {ethnologue_file}')


def downloadLanguageFamilies(file: Path) -> bool:
    langList = []
    lfCount = 0
    parser = etree.HTMLParser()

    baseUrl = 'https://www.ethnologue.com'
    langFamilyUrl = baseUrl + '/browse/families'

    response = requests.get(langFamilyUrl)
    if response:
        root = etree.parse(StringIO(response.text), parser)
        for element in root.iter('a'):
            href = element.get('href')
            if href is not None and href.startswith('/subgroups'):
                lfCount += 1
                tokens = re.match(r'(\D+) \(\d+\)', element.text)
                if tokens:
                    languageFamily = tokens.group(1)        # Strip off the language count from the name
                else:
                    languageFamily = element.text
                print(f'{lfCount}. Extracting language family {languageFamily}')
                lfDetailUrl = baseUrl + href
                time.sleep(1)
                response = requests.get(lfDetailUrl)
                if response:
                    lfDetailRoot = etree.parse(StringIO(response.text), parser)
                    for langElement in lfDetailRoot.iter('span'):
                        langClass = langElement.get('class')
                        if langClass is not None and langClass == 'field-content' and len(langElement):
                            language = langElement.text.strip()
                            isoCode = langCountry = ''
                            for langAttrib in langElement.iter('a'):
                                langHref = langAttrib.get('href')
                                if langHref:
                                    if langHref.startswith('/language'):
                                        isoCode = langAttrib.text[1: len(langAttrib.text) - 1]
                                    elif langHref.startswith('/country'):
                                        langCountry = langAttrib.text

                            langList.append({'languageFamily': languageFamily,
                                             'language': language,
                                             'isoCode': isoCode,
                                             'langCountry': langCountry,
                                             })

    writeLanguages(file, langList)
    return True


def loadLanguageFamilies(file: Path = None, force_download: bool = False) -> bool:
    global ethnologue_file
    global languages

    if len(languages) == 0:
        if file is None:
            file = ethnologue_file
        if not Path.is_file(file):
            if not force_download:
                print(f'ERROR: Language family file ({file}) does not exist')
                return False
            if not downloadLanguageFamilies(file):
                return False
        languages = loadLanguages(file)
        if len(languages) == 0:
            return False
        languages += loadLanguages(exceptions_file)
        if len(languages) == 0:
            return False

    return True


# Note: call this method with the language's ISO-639-3 code
def getLanguageFamily(iso639code: str):
    global languages

    # See if the language families have already been loaded; if not, load them
    if len(languages) == 0:
        loadLanguageFamilies()

    for lang in languages:
        # Does the specified ISO code match with this entry?
        if iso639code == lang.get('isoCode'):
            return lang['languageFamily']

    # Unrecognized language code
    return None


# Note: call this method with the language's ISO-639-3 code
def getLanguageDetails(iso639code: str) -> Tuple[str, str, str]:
    global languages

    # See if the language families have already been loaded; if not, load them
    if len(languages) == 0:
        loadLanguageFamilies(force_download=True)

    # Look up the language code
    for lang in languages:
        # Does the specified ISO code match with this entry?
        if iso639code == lang.get('isoCode'):
            return lang['language'], lang['languageFamily'], lang['langCountry']

    # Unrecognized language code
    return '', '', ''


def main() -> int:
    if not loadLanguageFamilies(force_download=True):
        return -1
    lang, langFam, langCountry = getLanguageDetails('aak')
    print(f'aak, {lang}, {langFam}, {langCountry}')
    return 0


if __name__ == "__main__":
    main()
