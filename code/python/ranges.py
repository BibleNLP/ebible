"""ranges.py tidies up files extracted by SIL Machine by removing empty verse ranges.
The fix_file function iterates though a file and removes lines that consist of only '<range>' or '...' where the line before is  empty. """

import argparse
import multiprocessing as mp
import os
from pathlib import Path


def fix_file(params):

    file, output_folder = params

    range_count = 0
    verse_count = 0
    empty_range = 0

    deletable_lines = set(["<range>", "..."])

    with open(file, "r", encoding="utf-8") as f_in:
        lines = f_in.readlines()

    for idx in range(1, len(lines) - 1):

        prev_line = lines[idx - 1].strip()
        line = lines[idx].strip()

        # Count verses with some text, including range markers.
        if line != "":
            verse_count += 1
            if line in deletable_lines:
                range_count += 1
                if prev_line == "":
                    empty_range += 1
                    verse_count -= 1
                    lines[idx] = ""

        # print(range_count, lines[idx-1],lines[idx])

    if verse_count > 0 and empty_range > 0:
        output_file = output_folder / file.name

        with open(output_file, "w", encoding="utf-8", newline="\n") as f_out:
            f_out.writelines(lines)

    return (verse_count, range_count, empty_range)


def main():
    parser = argparse.ArgumentParser(description="Removes empty ranges from extracts.")
    parser.add_argument("input", type=Path, help="The extract folder.")
    parser.add_argument(
        "output", type=Path, help="The folder for modified output files."
    )
    args = parser.parse_args()

    input = Path(args.input)
    output_folder = Path(args.output)
    files = [file for file in input.glob("*.txt")]
    empty_files = list()
    short_files = list()

    # Set processors to use
    cpus_to_use = 20
    print(f"Number of processors: {mp.cpu_count()} using {cpus_to_use}")
    pool = mp.Pool(cpus_to_use)

    # Iterate over files_found with multiple processors.
    counts = pool.map(fix_file, [(file, output_folder) for file in files])
    pool.close()

    total_verses = 0
    total_ranges = 0
    total_empty = 0
    files_with_empty_ranges = list()

    for file, counts in zip(files, counts):
        verse_count, range_count, empty_range = counts
        total_verses += verse_count
        total_ranges += range_count
        total_empty += empty_range
        if empty_range > 0:
            files_with_empty_ranges.append(file)

        if verse_count == 0:
            files_to_delete = [file, output_folder / file.name]
            for file_to_delete in files_to_delete:
                if os.path.exists(file_to_delete):
                    os.remove(file_to_delete)
                    if os.path.exists(file_to_delete):
                        print(
                            f"Attempted to delete empty file: {file_to_delete} but the attempt failed."
                        )
                    else:
                        print(f"Deleted empty file: {file_to_delete}")

        if verse_count < 200 :
            short_files.append(file)


        # files[file] = {"verse_count" :verse_count, "range_count": range_count,"empty_range":empty_range}

        # print(counts,file)

    print(f"There are {len(files)} files.")

    if len(files_with_empty_ranges) > 0:
        print(f"\nThese {len(files_with_empty_ranges)} files had empty ranges which were removed:")
        for file_with_empty_ranges in files_with_empty_ranges:
            print(file_with_empty_ranges)
        print()
    
    if len(short_files) > 0:
        print(f"\nThese {len(short_files)} have fewer than 200 verses.")
        for short_file in short_files:
            print(short_file)
        print()
    

    print(f"There are a total of {total_verses} verses.")
    print(
        f"There are a total of {total_ranges} ranges. {total_ranges/total_verses * 100:.3f}% of verses are ranges"
    )
    print(
        f"There are a total of {total_empty} empty ranges. {total_empty/total_verses * 100:.4f}% of verses are empty_ranges"
    )
    print(f"{total_empty/total_ranges * 100:.4f}% of ranges are empty_ranges.")


if __name__ == "__main__":
    main()