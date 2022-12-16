#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import multiprocessing as mp
import sys
import unicodedata
from collections import Counter, OrderedDict
from pathlib import Path
from script_data import script_data
global tokens

def remove_tokens(line, tokens):
    for token in tokens:
        line = line.replace(token, '')
    return line

def count_chars_mp(parameters):
    ''' The main function of char_freq is to count the number of times each character appears in files.
        This function reads a single file and returns a Counter for the characters in the file.
        The function only reads utf-8 files. Any non UTF-8 files will throw an error.
        Characters in the lines containing only the "<range>" marker are not counted.
    '''
    filename , tokens = parameters
    #print("count_chars entered!\n")
    #print(f"Processing file: {filename}")
    char_count = Counter()
    with open(filename, 'r', encoding='utf-8') as infile:
        lines = infile.readlines()
        
        if len(tokens) == 0:
            for line in lines:
                char_count.update(line)
        else:
            for line in lines:
                no_token_line = remove_tokens(line,tokens)
                char_count.update(no_token_line)

    return {filename:char_count}

def script(char):
    """ Return the script associated with the unicode character. """
    l = 0
    r = len(script_data['idx']) - 1
    c = ord(char)
    while r >= l:
        m = (l + r) >> 1
        if c < script_data['idx'][m][0]:
            r = m - 1
        elif c > script_data['idx'][m][1]:
            l = m + 1
        else:
            return script_data['names'][script_data['idx'][m][2]]
    return 'Unknown'

def unicode_data(character,count,file=""):
    """ With a single unicode character look up lots of information about it.
        Also deal with certain characters, like tab, end of line and commas since
        they mess up the formatting of a tsv file.
    """
    # TODO Posibly change this so that it takes only a character and returns the dictionary.
    # Possibly create the dictionary of all {char:info_dict} for each character found.
    # Then add in the filename and count as needed later.
    
    # The data is stored in a dictionary:
    c = OrderedDict()

    if not file == "" :
        c["filename"] = file.name

    #c["char"] = " " + character + " "
    c["char"] = character
    c["count"] = count
    c["code"] = ord(character)
    c["unicode_hex"] = f'{c["code"]:x}'.upper()
    try:
        c["name"] = unicodedata.name(character)
    except:
        c["name"] = "No name found."

    #Remove charatcers that mess up the tsv output - replace with a space.
    if character in ["	","\t", "\n", "\r", ","]  or ord(character) == 9:
        c["char"] = ' '
        c["name"] = " CHARACTER TABULATION"
    elif character == "," or ord(character) == 44:
        c["char"] = ' '
        c["name"] = "COMMA"
    elif character == "\n" or ord(character) == 10:
        c["char"] = ' '
        c["name"] = "END OF LINE"
    elif character == "\r" or ord(character) == 13:
        c["char"] = ' '
        c["name"] = "CARRIAGE RETURN"

    c["script"] = script(character)
    c["cat"] = unicodedata.category(character)
    c["bytes"] = len(character.encode('utf8'))

    c["bidi"] = unicodedata.bidirectional(character)
    c["combining"] = unicodedata.combining(character)
    c["eaw"] = unicodedata.east_asian_width(character)
    c["mirrored"] = unicodedata.mirrored(character)

    return c

def simple_unicode_data(character):
    """ With a single unicode character look up lots of information about it.
        Also deal with certain characters, like tab, end of line and commas since
        they mess up the formatting of a tsv file.
    """
    # TODO This is the attempt to change unicode_data so that it takes only a character and returns the dictionary.
    # Possibly create the dictionary of all {char:info_dict} for each character found.
    # Then add in the filename and count as needed later.
    
    # The data is stored in a dictionary:
    c = OrderedDict()

    c["char"] = character
    c["count"] = count
    c["code"] = ord(character)
    c["unicode_hex"] = f'{c["code"]:x}'.upper()
    try:
        c["name"] = unicodedata.name(character)
    except:
        c["name"] = "No name found."

    #Remove charatcers that mess up the tsv output - replace with a space.
    if character in ["	","\t", "\n", "\r", ","]  or ord(character) == 9:
        c["char"] = ' '
        c["name"] = " CHARACTER TABULATION"
    elif character == "," or ord(character) == 44:
        c["char"] = ' '
        c["name"] = "COMMA"
    elif character == "\n" or ord(character) == 10:
        c["char"] = ' '
        c["name"] = "END OF LINE"
    elif character == "\r" or ord(character) == 13:
        c["char"] = ' '
        c["name"] = "CARRIAGE RETURN"

    c["script"] = script(character)
    c["cat"] = unicodedata.category(character)
    c["bytes"] = len(character.encode('utf8'))

    c["bidi"] = unicodedata.bidirectional(character)
    c["combining"] = unicodedata.combining(character)
    c["eaw"] = unicodedata.east_asian_width(character)
    c["mirrored"] = unicodedata.mirrored(character)

    return c

def get_all_char_data(char_summary_tsv):
    
    with open(char_summary_tsv, 'r', newline='', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        all_char_dicts = {char_dict['char'] : char_dict for char_dict in reader}
    
    return all_char_dicts
        

def write_tsv(outfile, row_data, column_headers = [], overwrite = False):
    '''Write the data to a tsv file.
    If column headers are defined overwrite any existing file, so that the column headings are only written once
    at the top of the file.
    If there are no column headers defined then append the rows if there is an existing file.
    '''
    if not column_headers:
        column_headers = row_data.keys()

    if overwrite:
        with open(outfile, 'w', encoding='utf-8', newline='') as tsvfile:
            writer = csv.DictWriter(tsvfile, dialect='excel-tab', fieldnames=column_headers)
            writer.writeheader()
            writer.writerows(row_data)
    else:
        with open(outfile, 'a', encoding='utf-8', newline='') as tsvfile:
            writer = csv.DictWriter(tsvfile, dialect='excel-tab', fieldnames=column_headers)
            writer.writerows(row_data)


def get_character_data(char_counts,file=""):
    """ Given a Counter that has counted the occurrences of characters, return a list of dictionaries with
    lots of data about each character."""
    character_data = []
    for char, count in char_counts.most_common():
        c = unicode_data(char,count,file)
        character_data.append(c)
    return character_data
    

def main():
    
    parser = argparse.ArgumentParser(description="Write tsv reports about the characters found in multiple files.")
    parser.add_argument("--folder",  type=Path, default="../../corpus", help="Folder with corpus of Bible extracts")
    parser.add_argument("--files",   type=str, default="*-*.txt", help="Specify which files to read by extension. The default is: *-*.txt")
    parser.add_argument("--output",  type=Path, default="../../metadata/", help="Folder for the output results. The default is ../../metadata/")

    #parser.add_argument('--folder',  type=Path,                                                help="Folder to search")
    #parser.add_argument('--output_folder', type=Path,                                                help="Folder for the output results. The default is the current folder.", required=False)
    #parser.add_argument('--files',     type=str,            default="txt",                       help="Specify which files to read by extension. The default is 'txt'.")
    parser.add_argument('--input-files',   nargs="+",           default=[],                          help="Files to read. Ignores input folder and extension argument.")
    parser.add_argument('--split-token',   action="store_true", default=False,                       help="Count the indiviual characters in the <range> token.")
    parser.add_argument("--summary",       type=str,            default="character_summary.tsv",     help="The filename for the tsv summary file. The default is character_summary.tsv")
    parser.add_argument("--full",          type=str,            default="character_report.tsv",      help="The filename for the tsv report file. The default is character_report.tsv")
    parser.add_argument("--script",        type=str,            default="script_details.tsv",      help="The filename for the tsv report file. The default is character_report.tsv")
    
    # Command line to count characters in eBible
    # python charfreq.py --folder F:\GitHub\davidbaines\eBible\corpus --output_folder F:\GitHub\davidbaines\eBible\metadata 

    # Command line to count characters in Paratext extracts
    # python charfreq.py --folder "G:\Shared drives\Partnership for Applied Biblical NLP\Data\Corpora\Paratext_extracts_2022_11_03" --output_folder "G:\Shared drives\Partnership for Applied Biblical NLP\Data\Corpora\Paratext_metadata"

    #
    #python charfreq.py --folder F:\Corpora --output_folder F:\Corpora 
    args = parser.parse_args()

    split_token = args.split_token
    tokens = [] if split_token else ["<range>"]
        
    output_folder = Path(args.output)
    
    summary_tsv_file = output_folder / args.summary
    detail_tsv_file  = output_folder / args.full
    script_tsv_file = output_folder / args.script


    if len(args.input_files) > 0:
        files_found = sorted([Path(file) for file in args.input_files])[:1]
        print("Found the following files:")
        for file in files_found:
            print(file)
        
    elif args.folder:
        input_folder = args.folder
        extension    = args.files
        
        #path_split = Path(input_folder).parts
        #folder = path_split[-1]
        
        pattern = "".join([r"**\\", extension])
        files_found = sorted(input_folder.glob(pattern))#[:1]
        print(f"Found {len(files_found)} files with .{extension} extension in {input_folder}")

    else :
        print("Either --folder or --input_files must be specified.")
        exit(0)
          
    no_of_cpu = 20
    print(f"Number of processors: {mp.cpu_count()} using {no_of_cpu}")
    pool = mp.Pool(no_of_cpu)
    
    sys.stdout.flush()
    
    # Iterate over files_found with multiple processors.
    results = pool.map(count_chars_mp, [(file , tokens) for file in files_found])
    
    pool.close()
    #print(results, "\n" , type(results), "\n", len(results) )     
    #print(f"There are {len(results)} results\n")
    
    # Keep a running total of the characters seen across all files.
    all_chars = Counter()

    # Keep script and alphabet summary for each file
    file_scripts = list()
    file_data_column_headers = ['file', 'Main Script', 'alphabet']
    file_data_column_headers.extend(script_data['names'])
    file_data_column_headers.extend(['Unknown'])

    # print(file_data_column_headers)
    # print(script_data['names'])

    filecount = 0
    for result in results:

    #    print("This is a single result:")
    #    print(result)
    #    print(f"result is a :{type(result)}")
        for item in result.items():
            file, char_counter = item
            # print(f"{file}, {char_counter}\n")
            # Update the total character counts for all files.
            all_chars.update(char_counter)

            #List of dictionaries (one per char) with info.
            chars_list = get_character_data(char_counter,file)
            
            file_data = dict.fromkeys(file_data_column_headers)
            file_data['file'] = file.name
            alphabet = ''
            script_counter = Counter()

            for char_dict in chars_list:
                alphabet += char_dict['char']
                script_counter.update([char_dict['script']])
            file_data['alphabet'] = alphabet

            # This is probably redundant and messy.
            file_data['Main Script'] = script_counter.most_common(1)[0][0]
            
            for k,v in script_counter.items():
                file_data[k] = v            

            #print(char_dict)
            #print(file_data)
            file_scripts.append(file_data)
            if not all(item in file_data_column_headers for item in file_data.keys()):
                print(f"file_data: {file_data} has Unknown something.")
                print(file_data.keys())
                print(file_data_column_headers)

                exit()

            #Write out the data for this file to the detailled tsv file
            filecount += 1
            if filecount == 1:          #If it is the first writing then write the column headers.
                # Set column headers

                # This line adds the path to the files as the last column header.
                #chars_list[0][input_folder] = ''

                column_headers = chars_list[0].keys()
                
                with open(detail_tsv_file, 'w', encoding='utf-8', newline='') as tsvfile:
                    writer = csv.DictWriter(tsvfile, dialect='excel-tab', fieldnames=column_headers)
                    writer.writeheader()
                    
                    # And write the data for the first file
                    for char_dict in chars_list:
                        writer.writerow(char_dict)
                        
            else :   #For subsequent files just write the data.
                with open(detail_tsv_file, 'a', encoding='utf-8', newline='') as tsvfile:
                    writer = csv.DictWriter(tsvfile, dialect='excel-tab', fieldnames=column_headers)
                    for char_dict in chars_list:
                        writer.writerow(char_dict)
            
    print(f'Wrote detailed tsv file to {detail_tsv_file}')


    all_char_data = get_character_data(all_chars)
    column_headers = all_char_data[0].keys()
    write_tsv(summary_tsv_file, all_char_data, column_headers, overwrite=True)
    print(f'Wrote summary tsv file to {summary_tsv_file}')

    write_tsv(script_tsv_file, file_scripts, file_data_column_headers, overwrite=True)
    print(f'Wrote script summary tsv file to {script_tsv_file}')


    


if __name__ == "__main__":
    main()