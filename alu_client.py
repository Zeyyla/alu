#!/usr/bin/env python
# coding: utf-8

import numpy as np
import csv
import multiprocessing as mp
import time
from os import listdir
import pickle
from Task import Task
import boto3

def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def queue(tasks):
    while True:
        np.random.shuffle(tasks)
        for f in tasks:
            try:
                task = pickle.load(file=open("tasks/" + f, "rb"))
                if not task.finished():
                    yield task
            except:
                print("error with: " + str(f))
def run_task(awsTask):
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
    sqs_client = boto3.resource('sqs')