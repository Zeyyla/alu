import multiprocessing
import pickle
import numpy as np
import pandas as pd
from os import listdir
import csv
from Task import Task
from openpyxl import load_workbook

file = 'progress_summary.xlsx'
book = load_workbook(file)
writer = pd.ExcelWriter(file, engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
alu = ["AluJb", "AluJo", "AluJr", "AluJr4", "AluSc", "AluSc5", "AluSc8", "AluSg", "AluSg4", "AluSg7", "AluSp", "AluSq", "AluSq10", "AluSq2", "AluSq4", "AluSx", "AluSx1", "AluSx3", "AluSx4", "AluSz", "AluSz6", "AluY"]
genomes = [f for f in listdir("data") if "_" not in f]
genomes.remove('Orangutans')
tasks = [pickle.load(file=open("tasks/" + f, "rb")) for f in listdir("tasks")]
total = 0
completed = 0
for task in tasks:
    total += task.total
    completed += task.completed
print(f"completed {completed} out of {total} matches, {100 * completed / total}%")
# for f in listdir("tasks"):
#     try:
#         pickle.load(file=open("tasks/" + f, "rb"))
#     except:
#         print(f)
# writer = pd.ExcelWriter('progres_summary.xlsx')
summary = pd.DataFrame(index = genomes, columns = alu)

for a in alu:
    subtasks = [t for t in tasks if t.subfamily == a]
    p = np.zeros((len(genomes), len(genomes)))
    df = pd.DataFrame(p,index = genomes, columns = genomes)
    # print(df)
    for t in subtasks:
        #modify this if you want different data outputted
        df[t.species2][t.species1] = t.completed / t.total
        summary[a] = df.agg("mean", axis = "columns") / ((len(genomes)-1) / len(genomes))
    # df.to_excel(writer, sheet_name=a)
# workbook  = writer.book
# workbook.filename = 'progres_summary.xlsm'
# workbook.add_vba_project('vbaProject.bin')
# writer.save()
summary.to_excel(writer, "data")
writer.save()
