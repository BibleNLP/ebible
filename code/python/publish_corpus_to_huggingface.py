# Version 1 - uploading as JSON file to HuggingFace.
from clearml import Task
import time
import os
import shutil
import json
import csv
import pandas as pd
from tqdm import tqdm

if __name__ == '__main__':

    sync_time = str(int(time.time()))

    # connect to clearML
    task = Task.init(
        project_name='BibleNLP Corpus',    # project name of at least 3 characters
        task_name='HF-public-dataset-publishing-' + sync_time, # task name of at least 3 characters
        task_type="data_processing",
        tags=None,
        reuse_last_task_id=True,
        continue_last_task=False,
        auto_connect_arg_parser=True,
        auto_connect_frameworks=True,
        auto_resource_monitoring=True,
        auto_connect_streams=True,    
    )

    # work in temp dir
    tempdir = 'temp_' + sync_time
    os.mkdir(tempdir)
    curdir = os.getcwd()
    basedir = os.path.join(os.path.realpath('..'), 'corpus')
    vrefdir = os.path.join(os.path.realpath('..'), 'metadata')
    repodir = os.path.realpath('..')
    os.chdir(tempdir)

    # create dictionary from corpus
    all_bibles_as_lines = {}
    metadata = pd.read_excel(os.path.join(repodir,'metadata/Copyright and license information.xlsx'))
    meta_dict = {row[1]['translationId']:{'lic_type': row[1]['CC license url'], 
                                'copy': row[1]['Copyright']} for row in metadata.fillna('').iterrows()}
    with open(os.path.join(vrefdir,'vref.txt'), 'r') as txtfile:
        lines = txtfile.readlines()
        line2vref = {key:value.strip() for key,value in enumerate(lines)}
        vref2line = {v:k for k,v in line2vref.items()}

    for filename in tqdm(os.listdir(basedir)):
        lang = filename.split('-')[0]
        fileid = '-'.join(filename.split('-')[1:]).strip()
        fileid = fileid.split('.')[0]
        if filename != 'vref.txt': 
            if not all_bibles_as_lines.get(lang):
                all_bibles_as_lines[lang]=[]
            with open(os.path.join(basedir,filename), 'r') as txtfile:
                lines = txtfile.readlines()
                curr_dict = {}
                curr_dict['verses']=[]
                for idx, line in reversed(list(enumerate(lines))):
                    if line.strip() == '':
                        continue
                    else:
                        curr_dict['verses'].append(line2vref[idx])
                    if line.strip() != '<range>':
                        curr_dict['file'] = filename
                        curr_dict['text'] = line.strip()
                        if meta_dict.get(fileid, False):
                            curr_dict['license'] = f'{meta_dict[fileid]["lic_type"]}'
                            curr_dict['copyright'] = f'{meta_dict[fileid]["copy"]}'.strip()
                        else:
                            curr_dict['license'] = 'Not found in metadata'
                            curr_dict['copyright'] = 'Not found in metadata'
                        all_bibles_as_lines[lang].append(curr_dict)
                        curr_dict={}
                        curr_dict['verses'] = []

    # clone HF repo and branch
    os.system('git clone https://huggingface.co/datasets/bible-nlp/biblenlp-corpus')
    os.chdir('biblenlp-corpus')
    os.system('git checkout -b update-' + sync_time)

    # use git lfs
    os.system('git lfs install')
    os.system('git lfs track "*.json"')
    os.system('git add .gitattributes')

    # place dictionary in HF repo
    with open("corpus.json", "w", encoding='utf-8') as outfile:
        json.dump(all_bibles_as_lines, outfile, ensure_ascii=False, indent=4)

    # add, commit, and push
    os.system('git add corpus.json && git commit -m "update corpus" && git push -u origin update-' + sync_time)

    #clean up
    os.chdir(curdir)
    shutil.rmtree(tempdir)
    
