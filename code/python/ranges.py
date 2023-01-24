"""ranges.py tidies up files extracted by SIL Machine by removing empty verse ranges.
The fix_file function iterates though a file and removes lines that consist of only '<range>' or '...' where the line before is  empty. """

import argparse
import cmd
from collections import Counter
import multiprocessing as mp
import os
from pathlib import Path
from statistics import multimode
from typing import Tuple, List, OrderedDict


def choose_yes_no(prompt: str) -> bool:

    choice: str = " "
    while choice not in ["n", "y"]:
        choice: str = input(prompt).strip()[0].lower()
    if choice == "y":
        return True
    elif choice == "n":
        return False


def fix_file(params) -> Tuple[Path, Counter]:
    """fix_file function counts the number of verses with data, and number of ranges.
    Empty ranges are counted and removed, with the fixed file saved in the output folder..
    Results are returned as a Tuple containing the file and a counter."""

    file, output_folder = params

    count = Counter()
    # Can't initialise a counter to zero, since zero and negative values
    # Are automatically removed.  "ranges", "verses", "empty_ranges", "lines"

    deletable_lines = set(["<range>", "..."])

    with open(file, "r", encoding="utf-8") as f_in:
        lines = f_in.readlines()

    lines_input = len(lines)

    # Fix the first line if it is an empty range.
    if lines[0].strip() in deletable_lines:
        count["ranges"] == 1
        count["empty_ranges"] == 1
        lines[0] == "\n"
    else:
        count["verses"] == 1

    for idx in range(1, len(lines) - 1):

        prev_line = lines[idx - 1].strip()
        line = lines[idx].strip()

        # Count verses with some text, including range markers.
        if line != "":
            count["verses"] += 1
            if line in deletable_lines:
                count["ranges"] += 1
                if prev_line == "":
                    count["empty_ranges"] += 1
                    count["verses"] -= 1
                    lines[idx] = "\n"

    # Only write an output file if there are some verses and there are empty ranges
    if count["empty_ranges"] and count["verses"]:
        output_file = output_folder / file.name

        with open(output_file, "w", encoding="utf-8", newline="\n") as f_out:
            f_out.writelines(lines)

    if len(lines) != lines_input:
        print(
            f"Input File {file} has {lines_input} lines but the output has {len(lines)}."
        )
        print(
            f"Output files should have exactly the same number of lines as the input file."
        )
        print("Exiting.")
        return None

    count["lines"] = len(lines)

    if count["verses"] > count["lines"]:
        print("More verses than lines in file!")
        print(file.name, count)
        exit()

    return file, count


def fix_files(files, output_folder) -> OrderedDict:

    # Set processors to use
    cpus_to_use = mp.cpu_count() - 2
    print(f"Number of processors: {mp.cpu_count()} using {cpus_to_use}")
    pool = mp.Pool(cpus_to_use)
    # Iterate over files_found with multiple processors.
    count_info = pool.map(fix_file, [[file, output_folder] for file in files])
    pool.close()

    # print(f"In fix_files. count_info = {count_info}")
    # print(f"In fix_files. files_dict = {files_dict}")

    return {file: count for file, count in count_info}


def delete_empty_files(file_info) -> None:
    # File info is a dictionary with the file path as a key.
    # The values are a dictionary of verse and range counts.
    # The key required in this function is 'verse_count'

    for file, counts in file_info.items():

        if not counts["verse_count"] and os.path.exists(file):
            os.remove(file)
            if os.path.exists(file):
                print(f"Attempted to delete empty file: {file} but the attempt failed.")


def get_totals(verse_and_range_counts) -> Counter:

    total = Counter()

    for file, count in verse_and_range_counts.items():
        total["verses"] += count["verses"]
        total["ranges"] += count["ranges"]
        total["empty_ranges"] += count["empty_ranges"]
        total["lines"] += count["lines"]

    return total


def report_short_files(files_info, verses_threshold) -> None:

    short_files = [
        file
        for file, counts in files_info.items()
        if counts["verses"] <= verses_threshold
    ]

    if len(short_files) > 0:
        print(f"\nThese {len(short_files)} have {verses_threshold} or fewer verses.")
        for short_file in short_files:
            print(short_file.name)


def report_details(files_info,output_folder) -> None:

    files_with_empty_ranges = {
        file: counts for file, counts in files_info.items() if counts["empty_ranges"]
    }

    if len(files_with_empty_ranges) > 0:
        cli = cmd.Cmd()
        filenames = [file_with_empty_range.name for file_with_empty_range in files_with_empty_ranges]
        print(f"\nThese {len(files_with_empty_ranges)} files have empty ranges:")

        # import and use shutil.get_terminal_size().columns to adjust columns automatically.
        cli.columnize(filenames, displaywidth=80)
        print(f"\nModified versions without the empty ranges were saved in {output_folder.parent.resolve()}")
    else:
        print(f"\nThere were no files found containing empty ranges.")


    total = get_totals(files_info)
    total["files"] = len(files_info)

    print(f"\nThe totals are:")
    for k, v in total.most_common():
        key = k.replace("_", " ")
        print(f"{key:<14}  =  {v:>10}")

    print(f"{total['ranges']/total['verses'] * 100:>5.4f}% of verses are ranges")
    print(f"{total['empty_ranges']/total['verses'] * 100:>5.4f}% of verses are empty_ranges")
    print(f"{total['empty_ranges']/total['ranges'] * 100:>5.4f}% of ranges are empty_ranges.")


def main():
    parser = argparse.ArgumentParser(description="Removes empty ranges from extracts.")
    parser.add_argument("input", type=Path, help="The extract folder.")
    parser.add_argument("output", type=Path, help="The folder for modified files.")

    args = parser.parse_args()

    input_folder = Path(args.input)
    if not input_folder.is_dir():
        print(f"Can't find the input folder {input_folder}")
        exit()

    output_folder = Path(args.output)
    if not output_folder.is_dir():
        print(f"Can't find the output folder: {output_folder}")
        exit()

    files = [file for file in input_folder.glob("*." + "txt")]
    print(f"Found {len(files)} files in {input_folder} folder to check.")

    files_info = fix_files(files, output_folder)

    empty_files = {
        file: counts for file, counts in files_info.items() if "verses" not in counts
    }
    # print(f"These are the empty_files: {empty_files}")
    if empty_files:
        print(f"These empty files, have no verse text.")
        for file, counts in empty_files.items():
            print(file.name)
        print(
            f"There are {len(empty_files)} empty files without verse text.\nYou might like to check that they really are empty."
        )
        if choose_yes_no(f"Delete all the empty files? y/n"):
            delete_empty_files(empty_files)
    else: 
        print(f"\nThere were no files without some verse text.")
    

    report_details(files_info,output_folder)

if __name__ == "__main__":
    main()
