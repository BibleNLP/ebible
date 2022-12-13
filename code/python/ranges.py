import argparse
#from itertools import chain
import multiprocessing as mp
import os
from pathlib import Path


def fix_file(params):
    
    file, output_folder = params

    range_count = 0
    verse_count = 0
    empty_range = 0

    with open(file, 'r', encoding='utf-8') as f_in:
        lines = f_in.readlines()

    for idx in range(1, len(lines)-1):
        
        prev_line = lines[idx-1].strip()
        line = lines[idx].strip()

        # Count verses with some text, including range markers.
        if  line != '':
            verse_count += 1
            if line in ["<range>", "..."]:
                range_count += 1
                if prev_line == '':
                    empty_range += 1
                    verse_count -= 1
                    lines[idx] = ''

        #print(range_count, lines[idx-1],lines[idx])


    if verse_count > 0 and empty_range > 0 :
        output_file = output_folder / file.name

        with open(output_file, 'w', encoding='utf-8',newline='\n') as f_out:
            f_out.writelines(lines)
    
    return (verse_count, range_count, empty_range)


def main():
    parser = argparse.ArgumentParser(description="Removes empty ranges from extracts.")
    parser.add_argument("input", type=Path, help="The extract folder.")
    parser.add_argument("output", type=Path, help="The folder for modified output files.")
    args = parser.parse_args()

    input = Path(args.input)
    
    output_folder = Path(args.output)
    
    files = [file for file in input.glob("*.txt")]

    #for file in files[:10]:
    #    range_count, verse_count = fix_file(file)
    #    print(verse_count,range_count,file)

    #exit()

    # Set processors to use
    cpus_to_use = 20
    print(f"Number of processors: {mp.cpu_count()} using {cpus_to_use}")
    pool = mp.Pool(cpus_to_use)

    # Iterate over files_found with multiple processors.
    counts = pool.map(fix_file, [(file, output_folder) for file in files])
    pool.close()

    total_verses = 0
    total_ranges = 0
    total_empty  = 0
    files_with_empty = 0

    for file, counts in zip(files,counts):
        verse_count, range_count, empty_range = counts
        total_verses += verse_count
        total_ranges += range_count
        total_empty  += empty_range
        if empty_range > 0:
            files_with_empty += 1
        if verse_count == 0:
            os.remove(file) 
            if os.path.exists(file):
                print(f"File {file} contained no verse data but the attempt to delete it failed.")
            else:
                print(f"File {file} contained no verse data and was deleted.")


        #files[file] = {"verse_count" :verse_count, "range_count": range_count,"empty_range":empty_range}

        print(counts,file)

    print(f"There are {len(files)} files, of which {files_with_empty} have empty ranges.")
    print(f"There are a total of {total_verses} verses.")
    print(f"There are a total of {total_ranges} ranges. {total_ranges/total_verses * 100:.3f}% of verses are ranges")
    print(f"There are a total of {total_empty} empty ranges. {total_empty/total_verses * 100:.4f}% of verses are empty_ranges")
    print(f"{total_empty/total_ranges * 100:.4f}% of ranges are empty_ranges.")


if __name__ == "__main__":
    main()