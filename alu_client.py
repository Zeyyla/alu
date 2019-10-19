#!/usr/bin/env python
# coding: utf-8
#TODO: minify imports with as
import numpy as np
import multiprocessing as mp
from os import mkdir, rmdir, path, listdir
import pickle
from Task import Task, awsTask, SPECIES
import boto3
# import argparse
import json
from skbio.alignment import StripedSmithWaterman
from tqdm import tqdm
import heapq as hq
from functools import partial

def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

#TODO: messagefn with params

def run_task(awstask, params, pb):
    k_heap = []
    species1_records = json.load(open(params['datapath'] + awstask.species1 + "/" + awstask.species1 + "_" + awstask.subfamily + ".json", "rb"))
    species2_records = json.load(open(params['datapath'] + awstask.species2 + "/" + awstask.species2 + "_" + awstask.subfamily + ".json", "rb"))
    for i in awstask.indicies:
        sequence1 = species1_records[i]
        w = StripedSmithWaterman(str(sequence1['seq']), score_only=True)
        for sequence2 in species2_records:
            k_new = w(str(sequence2['seq']))["optimal_alignment_score"]
            match = sequence2    
            if len(k_heap) < 5: 
                hq.heappush(k_heap, (k_new, match['description'], match))
            elif k_new > k_heap[0][0]:
                hq.heapreplace(k_heap, (k_new, match['description'], match))
        if k_heap: 
            location1 = get_location(sequence1['description'])
            data = [[i], location1]
            for k in reversed(sorted(k_heap)):
                location2 = get_location(k[2]['description'])
                data.append(np.concatenate([location2, [k[0]]]))
        awstask.datas.append(list(np.concatenate(data)))
        pb.update(1)
    msg = json.dumps(awstask.__dict__)
    sqs = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'], region_name="us-east-2")
    response = sqs.send_message(QueueUrl=params['results_url'], MessageBody=msg)
    return response['MessageId']

def taskProcess(params, pos):
    print("Starting task {}".format(pos))
    while True:
        sqs = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'], region_name="us-east-2")
        response = sqs.receive_message(QueueUrl=params['task_url'], MaxNumberOfMessages=1, VisibilityTimeout=3600, WaitTimeSeconds=20)
        awstask = awsTask.fromDict(json.loads(response['Messages'][0]['Body']))
        desc = "{} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies))
        with tqdm(total=len(awstask.indicies), position=pos, desc=desc, unit="match") as pb:
            messageID = run_task(awstask, params, pb)
            sqs.delete_message(QueueUrl=params['task_url'], ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
            pb.set_description("Pushed {} - {} - {} with messageID: {}".format(awstask.species1, awstask.species2, awstask.subfamily, messageID))

def verify_local_data(params):
    dataPath = params['datapath']

    if dataPath[-1] != "/":
        dataPath += "/"

    if not path.exists(dataPath):
        mkdir(dataPath)
    
    s3 = boto3.resource('s3', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'], region_name="us-east-2")
    aludata = s3.Bucket('aludata')

    def download_data():
        print("Downloading data.zip from AWS.")
        import shutil
        shutil.rmtree(dataPath)
        mkdir(dataPath)
        from zipfile import ZipFile
        dataFile = aludata.Object("data.zip")
        with tqdm(total=dataFile.content_length, unit='B', unit_scale=True, desc="data.zip") as t:
            aludata.download_file("data.zip", path.join(dataPath, "data.zip"), Callback=t.update)
        with ZipFile(path.join(dataPath, "data.zip"), "r") as zip_file:
            zip_file.extractall(dataPath)
        print("New data extraction complete.")   
    
    #download json form server
    print("Downloading data_strucutre.json")
    aludata.download_file("data_structure.json", dataPath + "/data_structure.json")
    fileDict = json.load(open(dataPath + "/data_structure.json", "r"))

    #check if json matches filepaths
    download = False
    for s in tqdm(SPECIES, desc="Checking if files exist",position=0):
        if not path.exists(dataPath + s):
            download = True
            break
        for file, size in tqdm(zip(fileDict[s]['names'], fileDict[s]['lens']), desc=s, position=1, leave=False, total=len(fileDict[s]['names'])):
            filePath = path.join(dataPath, s, file)
            if not path.exists(filePath):
                download = True

    if download:
        print("Missing data files")
        download_data()
        return

    print("All files exist. Verifying file sizes...")
    for s in tqdm(SPECIES, desc="Progress verifying files",position=0):
        if download:
            break
        for file, lines in tqdm(zip(fileDict[s]['names'], fileDict[s]['lens']), desc=s, position=1, leave=False, total=len(fileDict[s]['names'])):
            filePath = path.join(dataPath, s, file)
            if len(json.load(open(filePath, "rb"))) != lines:
                download = True
                break
    
    if download:
        print("Malformed local data. Deleting and redownloading")
        download_data()
    
    print("All local data has been verified")

#TODO: Argument parsing https://stackabuse.com/command-line-arguments-in-python/
if __name__ == "__main__":
    paramFile = "awscredentials.json"
    with open(paramFile, "r") as f:
        params = json.load(f)

    #somehow verify_local_data interferes with other processes, so I'm isolating with with mp
    with mp.Pool(1) as p:
        r = p.map(verify_local_data, [params])

    num_processes = mp.cpu_count()//2 - 1
    with mp.Pool(num_processes) as p:
        p.map(partial(taskProcess, params), range(num_processes))
