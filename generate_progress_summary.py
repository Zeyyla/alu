import pickle
import numpy as np
import pandas as pd
from os import listdir
import csv
from Task import Task, SPECIES, SUBFAMILIES
from copy import copy
from openpyxl import load_workbook

file = 'progress_summary.xlsx'
book = load_workbook(file)
writer = pd.ExcelWriter(file, engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
genomes = copy(SPECIES)
genomes.remove('Orangutans')
summary = pd.DataFrame(index = genomes, columns = SUBFAMILIES)

for a in SUBFAMILIES:
    subtasks = [pickle.load(open("tasks/" + t, "rb")) for t in listdir("tasks/") if a == t.split("_")[2].split(".")[0]]
    p = np.zeros((len(genomes), len(genomes)))
    df = pd.DataFrame(p,index = genomes, columns = genomes)
    # print(df)
    for t in subtasks:
        #modify this if you want different data outputted
        df[t.species2][t.species1] = t.num_completed() / t.total
        summary[a] = df.agg("mean", axis = "columns") / ((len(genomes)-1) / len(genomes))
summary.to_excel(writer, "data")
writer.save()
