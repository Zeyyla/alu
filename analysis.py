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

def get_frequencies(species):
    records = SeqIO.parse("data/" + species + ".fasta", "fasta")
    freq = {}
    for r in records:
        subfamily = r.id.split("_")[2]
        freq[subfamily] = freq.get(subfamily, 0) + 1
    #uncomment these next three lines out if you want the relative frequencies
#     factor=1.0/sum(freq.values())
#     for sub in freq:
#         freq[sub] = freq[sub]*factor
    return freq

#return a list of sequences for a given species and subfamily
#ex: get_subfamily("humans", "AluJo")
def get_subfamily(species, subfamily):
    records = SeqIO.parse("data/" + species + ".fasta", "fasta")
    return [record for record in records if subfamily in record.id]

##return a list of sequences for a given species and subfamily on a chromosome
#ex: get_subfamily_chr("humans", "AluJo", 1)
def get_subfamily_chr(species, subfamily, chromosome):
    records = get_subfamily(species, subfamily)
    return [record for record in records if "chr"+str(chromosome) in record.description]

#retrieve a sequence for a given speicies based on its description
def get_sequence(species, description):
    records = SeqIO.parse("data/" + species + ".fasta", "fasta")
    return [record for record in records if description == record.description]

#parses location information from description string
def get_location(description):
    location = description.split(' ')[1].split(':')
    start, end = location[1].split('-')
    return [location[0].split("=")[1], int(start), int(end)]


def generate_pairings(species, subfamily="AluJo"):
    # print(species)
    species1 = species[0]
    species2 = species[1]
    species1_records = get_subfamily(species1, subfamily)
    species2_records = get_subfamily(species2, subfamily)
    filename = species1 + "_" + species2 + "_" + subfamily + ".csv"
    if not filename in listdir("data"):
        file=open(filename,"w")
        wr = csv.writer(file, quoting=csv.QUOTE_ALL)
        for sequence1 in species1_records:
            matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
            min_index = np.argmin(matches)
            match = species2_records[min_index]
            location1 = get_location(sequence1.description)
            location2 = get_location(match.description)
            data = np.concatenate([location1, location2, [min_index], [abs(location1[1] - location2[1])], [str(sequence1.seq)], [str(match.seq)]]) 
            wr.writerow(data)
        file.close()

def find_match(data):
    # print(data)
    sequence1 = data[0]
    species2 = data[1]
    subfamily = data[2]
    species2_records = pickle.load(open(species2 + "_" + subfamily + ".p", "rb"))
    matches = [editdistance.eval(str(sequence1.seq), str(sequence2.seq)) for sequence2 in species2_records]
    min_index = np.argmin(matches)
    match = species2_records[min_index]
    location1 = get_location(sequence1.description)
    location2 = get_location(match.description)
    data = np.concatenate([location1, location2, [matches[min_index]], [abs(location1[1] - location2[1])], [str(sequence1.seq)], [str(match.seq)]])
    # q.put(data)
    return data

def listener(filename, length, q):
    '''listens for messages on the q, writes to file. '''

    f = open(filename, 'w')
    wr = csv.writer(file, quoting=csv.QUOTE_ALL)
    pbar = tqdm(total = length)
    i = 0
    while 1:
        m = q.get()
        i += 1
        print(i)
        if m == 'kill':
            print('done')
            # f.write('killed')
            break
        wr.writerow(str(m))
        pbar.update()

        # f.flush()
    f.close()
    pbar.close()

if __name__ == "__main__":
    # lock = mp.Lock()
    def add_arguments():
        return ((sequence1, species2, subfamily) for sequence1 in species1_records)
    subfamily = "AluJo"
    print("here")
    genomes = [f.split(".")[0] for f in listdir("data") if ".fasta" in f]
    pairings = list(combinations(genomes, 2))
    for pairing in pairings:
        print(pairing)
        species1 = pairing[0]
        species2 = pairing[1]
        print("loading data...")
        if species1 + "_" + subfamily + ".p" in listdir():
            species1_records = pickle.load(open(species1 + "_" + subfamily + ".p", "rb"))
        else:
            species1_records = get_subfamily(species1, subfamily)
            pickle.dump(species1_records, open(species1 + "_" + subfamily + ".p", "wb"))
        if species2 + "_" + subfamily + ".p" in listdir():
            pass
            # species2_records = pickle.load(open(species2 + "_" + subfamily + ".p", "rb"))
        else:
            species2_records = get_subfamily(species2, subfamily)
            pickle.dump(species2_records, open(species2 + "_" + subfamily + ".p", "wb"))
        filename = species1 + "_" + species1 + "_" + subfamily + ".csv"
        print("starting mp")
        pbar = tqdm(total = len(species1_records),  unit="matches")
        with mp.Pool(mp.cpu_count() - 2) as p:
            with open(filename, 'w') as f:
                wr = csv.writer(f, quoting=csv.QUOTE_ALL)
                for result in p.imap(func=find_match, iterable=add_arguments()):
                    wr.writerow(result)
                    pbar.update()
        pbar.close()
        #     p.map(func=find_match, iterable=add_arguments())
        # manager = mp.Manager()
        # q = manager.Queue()    
        # pool = mp.Pool(mp.cpu_count() - 2)

        # #put listener to work first
        # watcher = pool.apply_async(listener, (filename,len(species1_records),q,))

        # #fire off workers
        # jobs = []
        # for i in add_arguments():
        #     job = pool.apply_async(find_match, (i, q))
        #     jobs.append(job)

        # # collect results from the workers through the pool result queue
        # for job in jobs: 
        #     job.get()

        # #now we are done, kill the listener
        # q.put('kill')
        # print('killed')
        # pool.close()
        # pool.join()

