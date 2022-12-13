import argparse
import os
import glob
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from ethnologue import getLanguageDetails


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect various counts from a corpus of Bible extracts")
    parser.add_argument("--folder", default="../../corpus", help="Folder with corpus of Bible extracts")
    parser.add_argument("--files", default="*-*.txt", help="Pattern of extract file names to count")
    parser.add_argument("--output", default="../../metadata/lang_details.tsv")
    args = parser.parse_args()

    files = []
    codes = []
    langs = []
    families = []
    countries = []
    extract_files_path = Path(args.folder, args.files)
    extract_files_list = glob.glob(str(extract_files_path))
    for extract_file_name in tqdm(extract_files_list):
        extract_base_name = os.path.basename(extract_file_name)
        lang_code = extract_base_name.split('-')[0]
        language, family, country = getLanguageDetails(lang_code)
        files.append(extract_base_name)
        codes.append(lang_code)
        langs.append(language)
        families.append(family)
        countries.append(country)

    # Initialize the data frame
    lang_details_df = pd.DataFrame()
    lang_details_df['file'] = files
    lang_details_df['code'] = codes
    lang_details_df['lang'] = langs
    lang_details_df['family'] = families
    lang_details_df['country'] = countries

    lang_details_df.to_csv(args.output, sep='\t', index=False)


if __name__ == "__main__":
    main()