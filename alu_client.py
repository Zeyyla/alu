#!/usr/bin/env python
# coding: utf-8
#TODO: minify imports with as
import numpy as np
import csv
import multiprocessing as mp
import time
from os import mkdir, path, listdir
import pickle
from Task import Task, SPECIES
import boto3
from sys import exit
import argparse
import json

#TODO: test with same credentials on computer

def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def run_task(awsTask, results_url, ACCESS_KEY, SECRET_KEY):
    ts = time.time()
    batch_size = min(50, task.remaining())
    # print(data)
    species1_records = pickle.load(open("data/" + awsTask.species1 + "/" + awsTask.species1 + "_" + awsTask.subfamily + ".p", "rb"))
    species2_records = pickle.load(open("data/" + awsTask.species2 + "/" + awsTask.species2 + "_" + awsTask.subfamily + ".p", "rb"))
    for i in awsTask.indicies:
        sequence1 = species1_records[i]
        k = 0
        match = None
        for sequence2 in species2_records:
            k_new = w(str(sequence2.seq))["optimal_alignment_score"]
            if k_new > k:
                match = sequence2
                k = k_new
        if match is not None:
            location1 = get_location(sequence1.description)
            location2 = get_location(match.description)
            data = np.concatenate([location1, location2, [k], [abs(location1[1] - location2[1])]])
            awsTask.datas.append(data)
    msg = pickle.dumps(awsTask)
    sqs = boto3.resource('sqs', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    sqs.send_message(QueueUrl=results_url, MessageBody=msg)

def taskProcess(crendentials):
    while True:
        sqs = boto3.client('sqs', aws_access_key_id=crendentials['aws_access_key_id'], aws_secret_access_key=crendentials['aws_secret_access_key'])
        response = sqs.receive_message(QueueUrl=task_url, MaxNumberOfMessages=1, VisibilityTimeout=20, WaitTimeSeconds=10)
        awsTask = pickle.loads(response['Messages'][0]['Body'])
        sqs.delete_message(QueueUrl=task_url, ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
        #get from dict
        run_task(awsTask, results_url, ACCESS_KEY, SECRET_KEY)

def verify_local_data(crendentials, dataPath = "data/"):
    if dataPath[-1] != "/":
        dataPath += "/"
    
    s3 = boto3.resource('s3', aws_access_key_id=crendentials['aws_access_key_id'], aws_secret_access_key=crendentials['aws_secret_access_key'])
    aludata = s3.Bucket('aludata')
    if not path.exists(dataPath):
        mkdir(dataPath)
    for s in SPECIES:
        if not path.exists(dataPath + s):
            mkdir(dataPath + s)
    filelist = {file.key for file in aludata.objects.all()}
    for file in filelist:
        #TODO: check if local file len matches server file len
        filepath = dataPath + file.split("_")[0] + "/" + file
        if not path.exists(filepath):
            print("Missing: " + filepath)
            aludata.download_file(file, filepath)
    
    filelist = {file for species_list in [listdir(dataPath + s) for s in SPECIES] for file in species_list}
    uploaded = {file.key for file in aludata.objects.all()}
    if filelist == uploaded:
        print("All server data has been verified")
    else:
        print("Error in verifying server data")
        exit()

#TODO: Argument parsing https://stackabuse.com/command-line-arguments-in-python/
if __name__ == "__main__":
    dataPath = "data/"
    credentialFile = "awscredentials.json"
    with open(credentialFile, "r") as f:
        credentials = json.load(f)
    verify_local_data(credentials, dataPath)
    processes = []
    #initialize all processes, then iterate and start
    # for i in range(mp.cpu_count()):
    #     processes[i] = mp.Process(target=taskProcess, args=)

    #test for full CPU utilization