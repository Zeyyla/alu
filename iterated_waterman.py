import numpy as np
import time
import pickle
from Task import Task
from skbio.alignment import StripedSmithWaterman
import edlib
import matplotlib.pyplot as plt

task = pickle.load(file=open("tasks/" + "Baboons_Bonobos_AluJb.p", "rb"))
species1_records = pickle.load(open("data/" + task.species1 + "/" + task.species1 + "_" + task.subfamily + ".p", "rb"))
species2_records = pickle.load(open("data/" + task.species2 + "/" + task.species2 + "_" + task.subfamily + ".p", "rb"))
for i in range(len(species1_records)):
    ts = time.time()
    k = 0 #approx number of base pairs in human genome
    sequence1 = str(species1_records[i].seq)
    w = StripedSmithWaterman(sequence1, score_only=True)
    match = None
    ks = []
    for sequence2 in species2_records:
        k_new = w(str(sequence2.seq))["optimal_alignment_score"]
        # k_new = edlib.align(sequence1, str(sequence2.seq), task = "editDistance")["editDistance"]
        ks.append(k_new)
        if k_new != -1 and k_new > k:
            match = sequence2
            # w = StripedSmithWaterman(sequence1, score_filter=k_new)
            k = k_new
    mean = np.mean(ks)
    std = np.std(ks)
    print(k, (k-mean)/std, time.time()-ts)
    # plt.hist(ks, bins=50)
    with open("distributions.p", "ab") as f:
        pickle.dump(ks, f)