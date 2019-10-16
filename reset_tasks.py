from itertools import product, combinations, permutations
from Task import Task, SPECIES, SUBFAMILIES
import json 

tasks = list(product(permutations(SPECIES, 2), SUBFAMILIES))
data_structure = json.load(open("data/data_structure.json", "r"))
for task in tasks:
	species1, species2, subfamily = task[0][0], task[0][1], task[1]
	species1_data = data_structure[species1]['names'].index(species1 + "_" + subfamily + ".p")
	total = data_structure[species1]['lens'][species1_data]
    t = Task(task[0][0], task[0][1], task[1], total)
    pickle.dump(t, open("tasks/"+t.filename(), "wb+"))
