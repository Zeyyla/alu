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
from os import listdir, path, mkdir, chdir
from itertools import combinations
import pickle
# from tqdm import tqdm
import edlib
# import time
from Task import Task
import sys

def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def queue(dir, tasks):
    while True:
        for f in tasks:
            task = pickle.load(file=open(dir + f, "rb"))
            if not task.finished():
                yield task
def run_task(task):
    ts = time.time()
    batch_size = min(50, task[0].remaining())
    # print(data)
    species1_records = task[1][task[0].completed:task[0].completed+batch_size]
    species2_records = task[2]
    datas = []
    for sequence1 in species1_records:
        # matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
        k = 300
        match = ""
        for sequence2 in species2_records:
            k_new = edlib.align(str(sequence1.seq), str(sequence2.seq), task = "editDistance", k = k)["editDistance"]
            if k_new != -1 and k_new < k:
                match = sequence2
                k = k_new
        # min_index = np.argmin(matches)
        # match = species2_records[min_index]
        location1 = get_location(sequence1.description)
        location2 = get_location(match.description)
        data = np.concatenate([location1, location2, [k], [abs(location1[1] - location2[1])]])
        datas.append(data)
    # print(datas)
    task[0].completed += batch_size
    with open("results/" + task[0].filename() + ".csv", 'a+', newline='') as file:
        wr = csv.writer(file, quoting=csv.QUOTE_ALL)
        wr.writerows(datas)
    pickle.dump(task[0], open("tasks/"+task[0].filename(), "wb"))
    print("completed {} matches for {} - {} - {} at {} seconds/match".format(batch_size, task[0].species1, task[0].species2, task[0].subfamily, round((time.time()-ts)/batch_size, 3)))
    return task

if __name__ == "__main__":
    species1 = "Humans"
    species2 = "Chimps"
    subfamily = "AluY"
    cores = mp.cpu_count() - 2
    task = Task(((species1, species2), subfamily))
    task = pickle.load(file=open("tasks/" + task.filename(), "rb"))
    tasks = []
    species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))
    species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
    if not path.isdir("complete_"+task.filename()) and sys.argv == "-1":
        tasks = [Task(((species1, species2), subfamily), i) for i in range(cores)]
        for i in range(10):
            tasks[i].completed = int(task.remaining()/cores)*i + task.completed
            tasks[i].total = int(task.remaining()/cores)*(1+i) + task.completed
            # print(tasks[i].remaining())
        tasks[cores-1].total = task.total
        mkdir("complete_"+task.filename())
        chdir("complete_"+task.filename())
        mkdir("results")
        mkdir("tasks")
        for t in tasks:
            print(t.filename(), t.completed, t.total, t.remaining())
            pickle.dump(t, open("tasks/"+t.filename(), "wb"))
    elif int(sys.argv[1]) >= 0:
        chdir("complete_"+task.filename())
        tasks = [pickle.load(file=open("tasks/" + f, "rb")) for f in listdir("tasks")]
        task = tasks[int(sys.argv[1])]
        print(task.filename(), task.completed, task.total, task.remaining())
        task = (task, species1_records, species2_records)
        while not task[0].finished():
            task = run_task(task)