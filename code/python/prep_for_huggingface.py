import pandas as pd
from pathlib import Path
import pyarrow # Required by pandas to write Parquet files
from tqdm import tqdm

# --- Configuration: Define Your Paths Here ---

# 1. Path to the vref.txt file
#    Example for Windows: vref_path = Path("F:/GitHub/BibleNLP/ebible/vref.txt")
#    Example for Linux/macOS: vref_path = Path("/home/user/github/BibleNLP/ebible/vref.txt")
vref_path = Path("F:/GitHub/BibleNLP/ebible/metadata/vref.txt") # <--- ADJUST THIS PATH

# 2. Path to the main corpus directory containing the .txt files
#    Example for Windows: corpus_dir = Path("F:/GitHub/BibleNLP/ebible/corpus")
#    Example for Linux/macOS: corpus_dir = Path("/home/user/github/BibleNLP/ebible/corpus")
corpus_dir = Path("F:/GitHub/BibleNLP/ebible/corpus") # <--- ADJUST THIS PATH

# 3. Path to the metadata Excel file
#    Example for Windows: metadata_path = Path("F:/GitHub/BibleNLP/ebible/metadata/eBible Corpus Metadata.xlsx")
#    Example for Linux/macOS: metadata_path = Path("/home/user/github/BibleNLP/ebible/metadata/eBible Corpus Metadata.xlsx")
metadata_path = Path("F:/GitHub/BibleNLP/ebible/metadata/eBible Corpus Metadata.xlsx") # <--- ADJUST THIS PATH

# 4. Directory where the output Parquet file will be saved
#    Example for Windows: output_dir = Path("F:/GitHub/BibleNLP/ebible/preprocessed_parquet")
#    Example for Linux/macOS: output_dir = Path("/home/user/github/BibleNLP/ebible/preprocessed_parquet")
output_dir = Path("F:/GitHub/BibleNLP/ebible/") # <--- ADJUST THIS PATH

# 5. Name for the output Parquet file
output_filename = "bible_corpus.parquet"

# --- End of Configuration ---

# --- Script Logic ---

# Ensure the output directory exists
output_dir.mkdir(parents=True, exist_ok=True)
output_parquet_path = output_dir / output_filename

# 1. Read vref.txt into a list
print(f"Reading verse references from: {vref_path}")
try:
    with open(vref_path, 'r', encoding='utf-8') as f_vref:
        # Assuming vref.txt lines correspond 1:1 with corpus lines, starting from line 1
        # Store as a list where index i corresponds to line i+1
        vref_ids = [line.strip() for line in f_vref]
    print(f"Successfully read {len(vref_ids)} verse references.")
except FileNotFoundError:
    print(f"Error: vref.txt not found at {vref_path}")
    exit() # Stop the script if vref.txt is missing
except Exception as e:
    print(f"Error reading {vref_path}: {e}")
    exit()

# 2. Read metadata to know which files to process and their language codes
print(f"Reading metadata from: {metadata_path}")
try:
    # Adjust sheet_name if your data is not on the first sheet
    # Ensure your Excel file has columns for the filename and the language code
    metadata_df = pd.read_excel(metadata_path, sheet_name=1)
    # --- IMPORTANT: Adjust these column names based on your Excel file ---
    filename_col = 'file' # Column with the filename (e.g., 'aaz-aaz.txt')
    lang_code_col = 'code' # Column with the language code (e.g., 'aaz', 'eng')
    # --- End of column name adjustment ---

    if filename_col not in metadata_df.columns:
        print(f"Error: Metadata file missing required column '{filename_col}'")
        exit()
    if lang_code_col not in metadata_df.columns:
        print(f"Error: Metadata file missing required column '{lang_code_col}'")
        exit()

    print(f"Successfully read metadata for {len(metadata_df)} translations.")

except FileNotFoundError:
    print(f"Error: Metadata file not found at {metadata_path}")
    exit()
except Exception as e:
    print(f"Error reading {metadata_path}: {e}")
    exit()


# 3. Process each translation file and aggregate data
corpus_data = {} # Dictionary to hold {verse_id: {lang_code1: text1, lang_code2: text2, ...}}

print("\nProcessing translation files...")
for index, row in tqdm(metadata_df.iterrows()):
    filename = row[filename_col]
    lang_code = row[lang_code_col]
    
    base_filename = Path(filename).stem

    # Construct the full path to the input .txt file
    input_txt_path = corpus_dir / filename

    if not input_txt_path.exists():
        print(f"Warning: Input file not found, skipping: {input_txt_path}")
        continue

    #print(f"Processing file {input_txt_path.name}")
    try:
        with open(input_txt_path, 'r', encoding='utf-8') as f_in:
            for i, line_text in enumerate(f_in):
                line_num = i + 1 # Line numbers usually start from 1
                text = line_text

                # Do not skip <range> markers and empty lines
                #if text == "<range>" or not text:
                #    continue

                # Get the corresponding verse_id from vref_ids list
                if i < len(vref_ids):
                    verse_id = vref_ids[i]

                    # Ensure verse_id entry exists in the main dictionary
                    if verse_id not in corpus_data:
                        corpus_data[verse_id] = {}

                   # Add the text for the current translation file
                    # Check for duplicates using base_filename (shouldn't happen if filenames are unique)
                    if base_filename in corpus_data[verse_id]:
                         # This warning is less likely now, but good to keep just in case
                         print(f"Warning: Duplicate entry for {verse_id} in file {base_filename}. Overwriting.")
                    corpus_data[verse_id][base_filename] = text
                else:
                    # This happens if a text file has more lines than vref.txt
                    print(f"Warning: Line {line_num} in {input_txt_path.name} exceeds vref.txt length ({len(vref_ids)} lines). Stopping processing for this file.")
                    break # Stop processing this specific file
    except Exception as e:
        print(f"Error processing file {input_txt_path.name}: {e}")
        # Decide if you want to continue with other files or exit
        # continue

# 4. Convert the aggregated data into a Pandas DataFrame
print("\nPutting data into DataFrame...")
# Create a list of dictionaries, where each dictionary represents a row
data_for_df = []
for verse_id, translations in corpus_data.items():
    row_data = {'verse_id': verse_id}
    row_data.update(translations) # Add {lang_code: text} pairs
    data_for_df.append(row_data)

if not data_for_df:
    print("Error: No data was processed. Please check input files and paths.")
    exit()

corpus_df = pd.DataFrame(data_for_df)

# Optional: Reorder columns to have verse_id first, then languages alphabetically
cols = ['verse_id'] + sorted([col for col in corpus_df.columns if col != 'verse_id'])
corpus_df = corpus_df[cols]

print(f"DataFrame created with {len(corpus_df)} rows and {len(corpus_df.columns)} columns.")
print("Sample rows:")
print(corpus_df.head())

# 5. Write the DataFrame to a Parquet file
print(f"\nWriting data to Parquet file: {output_parquet_path}")
try:
    # Using pyarrow engine is generally recommended
    # compression='snappy' is a good default, but you can choose others like 'gzip' or 'brotli'
    corpus_df.to_parquet(output_parquet_path, index=False, engine='pyarrow', compression='snappy')
    print("Successfully wrote Parquet file.")
except Exception as e:
    print(f"Error writing Parquet file: {e}")

print("\nPreprocessing finished.")
