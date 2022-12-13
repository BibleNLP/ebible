import argparse
import os
import glob
from pathlib import Path
from tqdm import tqdm
from collections import Counter
import pandas as pd

OT_canon = ['GEN', 'EXO', 'LEV', 'NUM', 'DEU', 'JOS', 'JDG', 'RUT', '1SA', '2SA', '1KI', '2KI', '1CH', '2CH',
            'EZR', 'NEH', 'EST', 'JOB', 'PSA', 'PRO', 'ECC', 'SNG', 'ISA', 'JER', 'LAM', 'EZK', 'DAN', 'HOS',
            'JOL', 'AMO', 'OBA', 'JON', 'MIC', 'NAM', 'HAB', 'ZEP', 'HAG', 'ZEC', 'MAL']
DT_canon = ['TOB', 'JDT', 'ESG', 'WIS', 'SIR', 'BAR', 'LJE', 'S3Y', 'SUS', 'BEL', '1MA', '2MA', '3MA', '4MA',
            '1ES', '2ES', 'MAN', 'PS2', 'ODA', 'PSS', 'EZA', 'JUB', 'ENO']
NT_canon = ['MAT', 'MRK', 'LUK', 'JHN', 'ACT', 'ROM', '1CO', '2CO', 'GAL', 'EPH', 'PHP', 'COL', '1TH', '2TH',
            '1TI', '2TI', 'TIT', 'PHM', 'HEB', 'JAS', '1PE', '2PE', '1JN', '2JN', '3JN', 'JUD', 'REV']


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect various counts from a corpus of Bible extracts")
    parser.add_argument("--folder", default="../../corpus", help="Folder with corpus of Bible extracts")
    parser.add_argument("--files", default="*-*.txt", help="Pattern of extract file names to count")
    parser.add_argument("--output", default="../../metadata/verse_counts.tsv")
    args = parser.parse_args()

    verse_counts = []
    extract_files_path = Path(args.folder, args.files)
    extract_files_list = glob.glob(str(extract_files_path))
    for extract_file_name in tqdm(extract_files_list):
        with open('../../metadata/vref.txt', 'r', encoding='utf-8') as vref_file,\
             open(extract_file_name, 'r', encoding='utf-8') as extract_file:
            book_list = []
            for vref, verse in zip(vref_file, extract_file):
                if verse == '\n':
                    continue
                book_list.append(vref.split(' ')[0])
            verse_counts.append({'file': os.path.basename(extract_file_name), 'counts': Counter(book_list)})

    # Initialize the data frame
    verse_count_df = pd.DataFrame()
    verse_count_df['file'] = [os.path.basename(extract_file_name) for extract_file_name in extract_files_list]
    verse_count_df = verse_count_df.set_index('file')
    for column in OT_canon + NT_canon + DT_canon:
        verse_count_df[column] = 0

    # Copy the counts to the data frame
    for totals in verse_counts:
        f = totals['file']
        counts = totals['counts']
        for ele in counts:
            verse_count_df.loc[f][ele] = counts[ele]

    verse_count_df.insert(loc=0, column='Books', value=verse_count_df.astype(bool).sum(axis=1))
    verse_count_df.insert(loc=0, column='Total', value=verse_count_df[OT_canon + NT_canon + DT_canon].sum(axis=1))
    verse_count_df.insert(loc=2, column='OT', value=verse_count_df[OT_canon].sum(axis=1))
    verse_count_df.insert(loc=3, column='NT', value=verse_count_df[NT_canon].sum(axis=1))
    verse_count_df.insert(loc=4, column='DT', value=verse_count_df[DT_canon].sum(axis=1))

    verse_count_df.to_csv(args.output, sep='\t')


if __name__ == "__main__":
    main()