from itertools import product, permutations
from Task import Task, SPECIES, SUBFAMILIES
import pickle
import json
import shutil
from os import mkdir
import csv

try:
	shutil.rmtree("tasks")
except FileNotFoundError:
	pass
try:
	shutil.rmtree("results")
except FileNotFoundError:
	pass
mkdir("results")
mkdir("tasks")
names = np.concatenate([['ind', 'c0', 's0', 'e0']]+[["c"+str(i), "s"+str(i), "e"+str(i), "sw"+str(i)] for i in range(1,6)])
tasks = list(product(permutations(SPECIES, 2), SUBFAMILIES))
data_structure = json.load(open("data/data_structure.json", "r"))
for task in tasks:
	species1, species2, subfamily = task[0][0], task[0][1], task[1]
	species1_data = data_structure[species1]['names'].index(species1 + "_" + subfamily + ".json")
	total = data_structure[species1]['lens'][species1_data]
	t = Task(species1, species2, subfamily, total)
	json.dump(t.getDict(), open("tasks/"+t.filename(), "w+"))
	wr = csv.writer(open("results/"+task.filename(), "w+", newline=''), quoting=csv.QUOTE_ALL)
        wr.writerow(names)
