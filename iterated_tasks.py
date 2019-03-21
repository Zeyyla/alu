#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Bio import SeqIO
from Bio import SeqFeature
from Bio.Alphabet import IUPAC
import pandas as pd
import editdistance
import numpy as np
import csv
import multiprocessing as mp
import time
import timeit
from os import listdir
from itertools import combinations
import pickle
from tqdm import tqdm
import edlib

class Task():
    def __init__(self,task):
        self.species1 = task[0][0]
        self.species2 = task[0][1]
        self.subfamily = task[1]
        self.total = len(pickle.load(file=open("data/"+self.species1 + "/" + self.species1+"_"+self.subfamily + ".p", "rb")))
        self.completed = 0
    def filename(self):
        return self.species1 + "_" + self.species2 + "_" + self.subfamily + ".p"
    def update(self,amount):
        self.completed += amount
    def finished(self):
        return self.completed == self.total
    def remaining(self):
        return self.total - self.completed
    def __eq__(obj):
        return (self.species1 == obj.species1) & (self.species2 == obj.species2) & (self.subfamily == obj.subfamily)
def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]

def run_task(task):
    batch_size = min(20, task.remaining())
    # print(data)
    species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))[task.completed:task.completed+batch_size]
    species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
    datas = []
    for sequence1 in species1_records:
        # matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
        matches = [edlib.align(str(sequence1.seq), str(sequence2.seq), task = "editDistance")["editDistance"] for sequence2 in species2_records]
        min_index = np.argmin(matches)
        match = species2_records[min_index]
        location1 = get_location(sequence1.description)
        location2 = get_location(match.description)
        data = np.concatenate([location1, location2, [matches[min_index]], [abs(location1[1] - location2[1])], [str(sequence1.seq)], [str(match.seq)]])
        datas.append(data)
    task.completed += batch_size
    # print("completed {} matches for {} - {} - {}".format(batch_size, task.species1, task.species2, task.subfamily))
    with open("results/" + task.filename() + ".csv", 'a+', newline='') as file:
        wr = csv.writer(file, quoting=csv.QUOTE_ALL)
        wr.writerows(datas)
    pickle.dump(task, open("tasks/"+task.filename(), "wb"))
    return task

if __name__ == "__main__":
        # lock = mp.Lock()
    tasks = [pickle.load(file=open("tasks/" + f, "rb")) for f in listdir("tasks")]
    average = np.median([t.completed for t in tasks])
    # filter tasks here if you want to split up computing
    tasks_to_run = []
    for task in tasks:
        # print("{} - {} - {}: {} / {}".format(task.species1, task.species2, task.subfamily, task.completed, task.total))
        if not task.finished():
            if task.species1 != "Humans" and task.species2 != "Humans":
                if task.completed <= average:
                    tasks_to_run.append(task)
    tasks_to_run.sort(key=lambda task: task.completed)
        # average = np.average([t.completed for t in tasks_to_run])
        # print(average)
    tasks_to_run = tasks_to_run[:100]
    while len(tasks_to_run) > 0:
        with mp.Pool(mp.cpu_count() - 2) as p:
            tasks_to_run = list(tqdm(p.imap(func=run_task, iterable=tasks_to_run),total=len(tasks_to_run),unit="batches"))
            # tasks_to_run = list(p.imap(func=run_task, iterable=tasks_to_run))
            tasks_to_run = [task for task in tasks_to_run if not task.finished()]          