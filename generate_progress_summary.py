import multiprocessing
import pickle
import numpy as np
import pandas as pd
from os import listdir
import csv
from iterated_tasks import Task
alu = ["AluJb", "AluJo", "AluJr", "AluJr4", "AluSc", "AluSc5", "AluSc8", "AluSg", "AluSg4", "AluSg7", "AluSp", "AluSq", "AluSq10", "AluSq2", "AluSq4", "AluSx", "AluSx1", "AluSx3", "AluSx4", "AluSz", "AluSz6", "AluY"]
genomes = [f for f in listdir("data") if "_" not in f]
tasks = [pickle.load(file=open("tasks/" + f, "rb")) for f in listdir("tasks")]

with pd.ExcelWriter('progres_summary.xlsx') as writer:
    for a in alu:
        subtasks = [t for t in tasks if t.subfamily == a]
        p = np.zeros((len(genomes), len(genomes)))
        df = pd.DataFrame(p,index = genomes, columns = genomes)
        # print(df)
        for t in subtasks:
            #modify this if you want different data outputted
            df[t.species2][t.species1] = t.completed / t.total
        df.to_excel(writer, sheet_name=a)

