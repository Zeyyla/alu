#!/usr/bin/env python
# coding: utf-8
#TODO: minify imports with as
import numpy as np
import multiprocessing as mp
from os import mkdir, rmdir, path, listdir
import pickle
from Task import Task, awsTask, SPECIES
import boto3
from sys import exit
# import argparse
import json
from skbio.alignment import StripedSmithWaterman
from tqdm import tqdm
import heapq as hq

#TODO: test with same credentials on computer


def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def run_task(awstask, results_url, ACCESS_KEY, SECRET_KEY, pb):
    k_heap = []
    heapq.heapify(k_heap) 
    species1_records = pickle.load(open("data/" + awstask.species1 + "/" + awstask.species1 + "_" + awstask.subfamily + ".p", "rb"))
    species2_records = pickle.load(open("data/" + awstask.species2 + "/" + awstask.species2 + "_" + awstask.subfamily + ".p", "rb"))
    for i in awstask.indicies:
        sequence1 = species1_records[i]
        w = StripedSmithWaterman(str(sequence1.seq), score_only=True)
        for sequence2 in species2_records:
            k_new = w(str(sequence2.seq))["optimal_alignment_score"]
            match = sequence2    
            if not k_heap: 
                hq.heappush(k_heap, (k_new, match))
            else: 
                min_k = min(k_heap)[0]
                if k_new > min_k:
                    hq.heapreplace(k_heap, (k_new, match))
        if k_heap: 
            data = [[i], location1]
            location1 = get_location(sequence1.description)
            for k in reversed(k_heap):
                location2 = get_location(k[1].description)
                data.append(np.concatenate([location2, [k]]))
        awstask.datas.append(list(np.concatenate(data)))
        pb.update(1)
    # print("Completed task: {} - {} - {} with {} indicies. Pushing now...".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies)))
    msg = json.dumps(awstask.__dict__)
    sqs = boto3.client('sqs', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name="us-east-2")
    response = sqs.send_message(QueueUrl=results_url, MessageBody=msg)
    # print("\tTask pushed with messageID: {}".format(response['MessageId']))
    return response['MessageId']

def taskProcess(credentials, pos):
    while True:
        sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'], region_name="us-east-2")
        response = sqs.receive_message(QueueUrl=credentials['task_url'], MaxNumberOfMessages=1, VisibilityTimeout=3600, WaitTimeSeconds=20)
        awstask = awsTask.fromDict(json.loads(response['Messages'][0]['Body']))
        # print("Recieved task: {} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies)))
        desc = "{} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies))
        with tqdm(total=len(awstask.indicies), position=pos, desc=desc, unit="match") as pb:
            messageID = run_task(awstask, credentials['results_url'], credentials['aws_access_key_id'], credentials['aws_secret_access_key'], pb)
            sqs.delete_message(QueueUrl=credentials['task_url'], ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
            pb.set_description("Pushed {} - {} - {} with messageID: {}".format(awstask.species1, awstask.species2, awstask.subfamily, messageID))
            # print("\tTask complete and deleted") 

def verify_local_data(credentials, dataPath = "data/"):
    if dataPath[-1] != "/":
        dataPath += "/"

    if not path.exists(dataPath):
        mkdir(dataPath)
    
    s3 = boto3.resource('s3', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'], region_name="us-east-2")
    aludata = s3.Bucket('aludata')
    
    #download json form server
    aludata.download_file("data_structure.json", dataPath + "/data_structure.json")
    fileDict = json.load(open(dataPath + "/data_structure.json", "r"))
    
    #check if json matches filepaths
    download = False
    for s in tqdm(SPECIES, desc="Progress verifying species:",position=0):
        if download:
            break
        if not path.exists(dataPath + s):
            download = True
            break
        for file, size in tqdm(zip(fileDict[s]['names'], fileDict[s]['lens']), desc=s, position=1, leave=False, total=len(fileDict[s]['names'])):
            filePath = path.join(dataPath, s, file)
            if not path.exists(filePath):
                download = True
                break
            if len(pickle.load(open(filePath, "rb"))) != size:
                download = True
                break
    
    if download:
        print("Missing or malformed local data. Deleting and redownloading")
        import shutil
        shutil.rmtree(dataPath)
        mkdir(dataPath)
        from zipfile import ZipFile
        aludata.download_file("data.zip", path.join(dataPath, "data.zip"))
        with ZipFile(path.join(dataPath, "data.zip"), "r") as zip_file:
            zip_file.extractall(dataPath)
        #unzip
    
    print("All local data has been verified")

#TODO: Argument parsing https://stackabuse.com/command-line-arguments-in-python/
if __name__ == "__main__":
    dataPath = "data/"
    credentialFile = "awscredentials.json"
    with open(credentialFile, "r") as f:
        credentials = json.load(f)
    verify_local_data(credentials, dataPath)
    # processes = []
    # # initialize all processes, then iterate and start
    # for i in range(mp.cpu_count()//2 - 1):
    #     processes.append(mp.Process(target=taskProcess, args=(credentials, i)))

    # for p in processes:
    #     p.start()
