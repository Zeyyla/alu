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

### set these parameters:
genome_from = "Humans"      # from "" to everything else
n_matches = np.inf             # limit the number of matches to n, set to np.inf to take everything
random_subsample = False    # randomly subsample our matches
###

genomes.remove(genome_from)
tasks = [t for t in tasks if t.species1==genome_from]

# iterate over all subfamilies
for a in alu:
    strings = []

    #only take from tasks that we have data for
    selected_tasks = []
    for g in genomes:
        [selected_tasks.append(t) for t in tasks if t.species2==g and t.subfamily==a]

    # determine the smallest number of matches we have available for this subfamily
    row=np.inf
    for t in selected_tasks:
        row_count = sum(1 for row in csv.reader(open("results/"+t.filename()+".csv", "r")))
        row = min(row, row_count)
    row = min(row, n_matches)

    # if subsampling, randomly shuffle our data
    if random_subsample:
        permutation = np.random.permutation(row)
    else:
        permutation = np.arange(row)

    # select the strings 
    df = pd.read_csv("results/"+selected_tasks[0].filename()+".csv", header=None)
    strings.append(df[df.columns[8]][permutation][:row].str.cat())
    for t in selected_tasks:
        df = pd.read_csv("results/"+t.filename()+".csv", header=None)
        strings.append(df[df.columns[9]][permutation][:row].str.cat())
        
    # write to file
    with open("clustal/"+a+"fasta", "w+", newline="") as file:
        wr = csv.writer(file)
        wr.writerow([">"+genome_from+"_"+a])
        wr.writerow([strings[0]])
        for i in range(len(selected_tasks)):
            wr.writerow([">"+selected_tasks[i].species2+"_"+a])
            wr.writerow([strings[i+1]])