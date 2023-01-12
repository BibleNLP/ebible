## Download Ebible Corpus

import os
import requests
import pandas as pd
from tqdm import tqdm
import daiquiri

#Create logger
module_logger = daiquiri.getLogger(__name__)

eBible_url = 'https://ebible.org/Scriptures/'
eBible_csv_url = 'https://ebible.org/Scriptures/translations.csv'
base_folder = 'ebible_download'
# Set the user-agent to Chrome for Requests.
headers = {"user-agent": "Chrome/51.0.2704.106"}

file_suffix = "_usfm.zip"
csv_headers = [
    "ID",
    "File",
    "Language",
    "Dialect",
    "Licence Type",
    "Licence Version",
    "CC Licence Link",
    "Copyright Holder",
    "Copyright Years",
    "Translation by",
]

#Make folders
downloads = os.path.join(base_folder,'downloads')
metadata = os.path.join(base_folder,'metadata')
metadata_csv = os.path.join(base_folder,'metadata','translations.csv')
logs = os.path.join(base_folder,'logs')
for f in [base_folder,downloads,metadata,logs]:
    os.makedirs(f,exist_ok=True)

# Download the list of translations.
module_logger.info('Starting download of eBible files...')
module_logger.info(f'Downloading list of translations from {eBible_csv_url} to: {metadata_csv}')

r = requests.get(eBible_csv_url, headers=headers)
if not r.ok:
    module_logger.error(f'Unable to read {eBible_csv_url}. Status code: {r.status_code}')
with open(metadata_csv, "wb") as out_file:
    out_file.write(r.content)

#Load, refactor translation df
meta_df = pd.read_csv(metadata_csv)
module_logger.info(f'Translation metadata df shape: {meta_df.shape}')
meta_df['Redistributable'] = meta_df['Redistributable'].astype('bool')
meta_df['filenames'] = meta_df['translationId']+file_suffix
meta_df = meta_df.sort_values(by=['filenames'])
module_logger.info(f'The translations csv file lists {len(meta_df)} translations.')
redist_count = len(meta_df[meta_df['Redistributable']==True])
module_logger.info(f'{redist_count} ({float(redist_count)/len(meta_df)*100}%) of translations are redistributable.')

#Determine files yet to be parsed
remaining_files = [i for i in meta_df['filenames'].tolist() if i not in os.listdir(downloads)]
module_logger.info(f'# Remaining files: {len(remaining_files)}')

#Download remaining files
queue = tqdm(remaining_files,desc='Zip File: '.ljust(25))
for f in queue:
    queue.set_description(f'Zip File: {f}'.ljust(25))
    r = requests.get(f'{eBible_url}{f}', headers=headers)
    if not r.ok:
        module_logger.error(f'Unable to read {eBible_url}{f}. Status code: {r.status_code}')
    with open(os.path.join(downloads,f), "wb") as out_file:
        out_file.write(r.content)

module_logger.info(f'Completed parsing {len(os.listdir(downloads))} of {len(meta_df)} files.')