import pandas as pd
from pathlib import Path
import pyarrow # Required by pandas to write Parquet files
from tqdm import tqdm

# --- Configuration: Define Your Paths Here, adjust as necessary ---
ebible_repo = Path("F:/GitHub/BibleNLP/ebible") 
ebible_repo_metadata = ebible_repo / "metadata"
ebible_repo_corpus = ebible_repo / "corpus"

# 1. Path to the vref.txt file
vref_path = ebible_repo_metadata / "vref.txt"

# 2. Path to the main corpus directory containing the .txt files
corpus_dir = ebible_repo / "corpus"

# 3. Path to the metadata Excel file
metadata_path = ebible_repo_metadata / "eBible Corpus Metadata.xlsx"

# 4. Directory where the output Parquet files will be saved
output_dir = Path("F:/GitHub/BibleNLP/ebible/") # <--- ADJUST THIS PATH

# 5. Name for the main output Parquet file
output_corpus_filename = "bible_corpus.parquet"

# 6. Name for the metadata output Parquet file
output_metadata_filename = "bible_metadata.parquet"

# --- End of Configuration ---

# --- Script Logic ---

# Ensure the output directory exists
output_dir.mkdir(parents=True, exist_ok=True)
output_corpus_parquet_path = output_dir / output_corpus_filename
output_metadata_parquet_path = output_dir / output_metadata_filename # <--- NEW

# 1. Read vref.txt into a list
print(f"Reading verse references from: {vref_path}")
try:
    with open(vref_path, 'r', encoding='utf-8') as f_vref:
        vref_ids = [line.strip() for line in f_vref]
    print(f"Successfully read {len(vref_ids)} verse references.")
except FileNotFoundError:
    print(f"Error: vref.txt not found at {vref_path}")
    exit()
except Exception as e:
    print(f"Error reading {vref_path}: {e}")
    exit()

# 2. Read metadata to know which files to process and their language codes
print(f"Reading metadata from: {metadata_path}")
try:
    # Adjust sheet_name if your data is not on the first sheet (e.g., sheet_name=1 for the second sheet)
    metadata_df = pd.read_excel(metadata_path, sheet_name=1) # Assuming data is on the second sheet

    # --- IMPORTANT: Adjust these column names based on your Excel file ---
    filename_col = 'file'          # Column with the filename (e.g., 'aaz-aaz.txt')
    lang_code_col = 'code'         # Column with the language code (e.g., 'aaz', 'eng')
    lang_name_col = 'lang'     # <--- NEW: Column with the English Language Name
    lang_family_col = 'family'     # <--- NEW: Column with the Language Family
    country_col = 'country'        # <--- NEW: Column with the Country
    verse_count_col = 'Total'     # <--- NEW: Column with the Total Verse Count
    # --- End of column name adjustment ---

    # Check if all required columns exist
    required_cols = [filename_col, lang_code_col, lang_name_col, lang_family_col, country_col, verse_count_col]
    missing_cols = [col for col in required_cols if col not in metadata_df.columns]
    if missing_cols:
        print(f"Error: Metadata file missing required columns: {', '.join(missing_cols)}")
        exit()

    print(f"Successfully read metadata for {len(metadata_df)} translations.")

except FileNotFoundError:
    print(f"Error: Metadata file not found at {metadata_path}")
    exit()
except Exception as e:
    print(f"Error reading {metadata_path}: {e}")
    exit()


# 3. Process each translation file and aggregate data
corpus_data = {} # Dictionary to hold {verse_id: {base_filename1: text1, base_filename2: text2, ...}}
metadata_list = [] # <--- NEW: List to store metadata for the second parquet file

print("\nProcessing translation files...")
# Use tqdm to show a progress bar
for index, row in tqdm(metadata_df.iterrows(), total=metadata_df.shape[0], desc="Processing Files"):
    filename = row[filename_col]
    lang_code = row[lang_code_col]
    base_filename = Path(filename).stem # e.g., 'eng-engWEB'

    # --- NEW: Store metadata for this file ---
    file_metadata = {
        'translation_id': base_filename, # Use base_filename to link with corpus columns
        'language_code': lang_code,
        'language_name': row[lang_name_col],
        'language_family': row[lang_family_col],
        'country': row[country_col],
        'verse_count': row[verse_count_col]
    }
    metadata_list.append(file_metadata)
    # --- End of new metadata storage ---

    # Construct the full path to the input .txt file
    input_txt_path = corpus_dir / filename

    if not input_txt_path.exists():
        # Use tqdm.write for messages within the loop to avoid messing up the progress bar
        tqdm.write(f"Warning: Input file not found, skipping: {input_txt_path}")
        continue

    try:
        with open(input_txt_path, 'r', encoding='utf-8') as f_in:
            for i, line_text in enumerate(f_in):
                line_num = i + 1
                # --- Don't strip whitespace---
                text = line_text.rstrip()

                # Skip <range> markers and empty lines if needed (currently commented out)
                # if text == "<range>" or not text:
                #    continue

                if i < len(vref_ids):
                    verse_id = vref_ids[i]
                    if verse_id not in corpus_data:
                        corpus_data[verse_id] = {}

                    if base_filename in corpus_data[verse_id]:
                         tqdm.write(f"Warning: Duplicate entry for {verse_id} in file {base_filename}. Overwriting.")
                    corpus_data[verse_id][base_filename] = text
                else:
                    tqdm.write(f"Warning: Line {line_num} in {input_txt_path.name} exceeds vref.txt length ({len(vref_ids)} lines). Stopping processing for this file.")
                    break
    except Exception as e:
        tqdm.write(f"Error processing file {input_txt_path.name}: {e}")
        # continue # Decide if you want to continue with other files

# 4. Convert the aggregated corpus data into a Pandas DataFrame
print("\nPutting corpus data into DataFrame...")
data_for_df = []
# Use tqdm for DataFrame creation progress
for verse_id, translations in tqdm(corpus_data.items(), desc="Creating Corpus DataFrame"):
    row_data = {'verse_id': verse_id}
    row_data.update(translations)
    data_for_df.append(row_data)

if not data_for_df:
    print("Error: No corpus data was processed. Please check input files and paths.")
    exit()

corpus_df = pd.DataFrame(data_for_df)

# Optional: Reorder columns
cols = ['verse_id'] + sorted([col for col in corpus_df.columns if col != 'verse_id'])
corpus_df = corpus_df[cols]

print(f"Corpus DataFrame created with {len(corpus_df)} rows and {len(corpus_df.columns)} columns.")
print("Sample corpus rows:")
print(corpus_df.head())

# 5. Write the Corpus DataFrame to a Parquet file
print(f"\nWriting corpus data to Parquet file: {output_corpus_parquet_path}")
try:
    corpus_df.to_parquet(output_corpus_parquet_path, index=False, engine='pyarrow', compression='snappy')
    print("Successfully wrote corpus Parquet file.")
except Exception as e:
    print(f"Error writing corpus Parquet file: {e}")

# --- NEW: Step 6 - Process and Write Metadata Parquet File ---
print("\nPutting metadata into DataFrame...")
if not metadata_list:
     print("Warning: No metadata was collected. Skipping metadata Parquet file creation.")
else:
    metadata_output_df = pd.DataFrame(metadata_list)
    print(f"Metadata DataFrame created with {len(metadata_output_df)} rows and {len(metadata_output_df.columns)} columns.")
    print("Sample metadata rows:")
    print(metadata_output_df.head())

    print(f"\nWriting metadata to Parquet file: {output_metadata_parquet_path}")
    try:
        metadata_output_df.to_parquet(output_metadata_parquet_path, index=False, engine='pyarrow', compression='snappy')
        print("Successfully wrote metadata Parquet file.")
    except Exception as e:
        print(f"Error writing metadata Parquet file: {e}")
# --- End of New Step 6 ---

print("\nPreprocessing finished.")
