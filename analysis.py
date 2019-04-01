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
from itertools import combinations, repeat
import pickle
from tqdm import tqdm
from iterated_tasks import Task
import edlib
import editdistance
import sys
def run_task(task):
    # ts = time.time()
    # species1_records = task[1]
    # species2_records = task[2]
    # batch_size = task[3]
    # task = task[0]
    # for sequence1 in species1_records:
    #     # matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
    #     k = np.inf
    #     match = ""
    #     for sequence2 in species2_records:
    #         k_new = edlib.align(str(sequence1.seq), str(sequence2.seq), task = "editDistance", k = k)["editDistance"]
    #         if k_new != -1 and k_new < k:
    #             match = sequence2
    #             k = k_new
    #             # print(k_new)
    #     # matches = [edlib.align(str(sequence1.seq), str(sequence2.seq), task = "editDistance")["editDistance"] for sequence2 in species2_records]
    # print("completed {} matches for {} - {} - {} at {} seconds/match".format(batch_size, task.species1, task.species2, task.subfamily, round((time.time()-ts)/batch_size, 3)))
    # return task
    ts = time.time()
    batch_size = min(100, task.remaining())
    # print(data)
    species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))[task.completed:task.completed+batch_size]
    species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
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
    print("completed {} matches for {} - {} - {} at {} seconds/match".format(batch_size, task.species1, task.species2, task.subfamily, round((time.time()-ts)/batch_size, 3)))

if __name__ == "__main__":
    print(sys.argv)
    # lock = mp.Lock()
    tasks = [Task((('Humans', 'Chimps'), 'AluY')) for _ in range(10)]
    for i in range(10):
        tasks[i].completed = 100*i
        tasks[i].total = 100*(1+i)
        print(tasks[i].remaining())
    # batch_size = 10
    # species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))[task.completed:task.completed+batch_size]
    # species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
    # task = (task, species1_records, species2_records, batch_size)
    # tasks = repeat(task, 10)
    with mp.Pool(mp.cpu_count() - 1) as p:
        tasks_to_run = p.imap(func=run_task, iterable=tasks)
        # tasks_to_run = list(p.imap(func=run_task, iterable=tasks_to_run))
        # tasks_to_run = [task for task in tasks_to_run if not task.finished()]
        for t in tasks_to_run:
            pass
