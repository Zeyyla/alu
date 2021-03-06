#!/usr/bin/env python
# coding: utf-8

# In[1]:


# from Bio import SeqIO
# from Bio import SeqFeature
# from Bio.Alphabet import IUPAC
# import pandas as pd
# import editdistance
import numpy as np
import csv
import multiprocessing as mp
import time
# import timeit
from os import listdir
import pickle
# from tqdm import tqdm
import edlib
# import time
from Task import Task

def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def cycle(tasks):
    while tasks:
        for task in tasks:
            if not task.finished():
                yield task
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
def run_task(task):
    ts = time.time()
    batch_size = min(50, task.remaining())
    # print(data)
    species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))[task.completed:task.completed+batch_size]
    species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
    datas = []
    for sequence1 in species1_records:
        # matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
        k = 10**9 #approx number of base pairs in human genome
        match = None
        for sequence2 in species2_records:
            k_new = edlib.align(str(sequence1.seq), str(sequence2.seq), task = "editDistance", k = k)["editDistance"]
            if k_new != -1 and k_new < k:
                match = sequence2
                k = k_new
        # min_index = np.argmin(matches)
        # match = species2_records[min_index]
        if match is not None:
            location1 = get_location(sequence1.description)
            location2 = get_location(match.description)
            data = np.concatenate([location1, location2, [k], [abs(location1[1] - location2[1])]])
            datas.append(data)
    # print(datas)
    task.update(batch_size)
    with open("results/" + task.filename() + ".csv", 'a+', newline='') as file:
        wr = csv.writer(file, quoting=csv.QUOTE_ALL)
        wr.writerows(datas)
    pickle.dump(task, open("tasks/"+task.filename(), "wb"))
    print("completed {} matches for {} - {} - {} at {} seconds/match".format(batch_size, task.species1, task.species2, task.subfamily, round((time.time()-ts)/batch_size, 3)))
    return task