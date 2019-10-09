import pickle
import random
from copy import deepcopy

class Task():
    def __init__(self, task, subsection = None):
        self.species1 = task[0][0]
        self.species2 = task[0][1]
        self.subfamily = task[1]
        self.total = len(pickle.load(file=open("data/"+self.species1 + "/" + self.species1+"_"+self.subfamily + ".p", "rb")))
        self.completed = {}
        self.remaining = {i for i in range(self.total)}
        self.candidates = {i for i in range(self.total)}
        self.subsection = subsection
    def filename(self):
        if self.subsection is None:
            return self.species1 + "_" + self.species2 + "_" + self.subfamily + ".p"
        else:
            return self.species1 + "_" + self.species2 + "_" + self.subfamily + "_" + str(self.subsection) + ".p"
    def update(self,awsTask):
        self.completed.update(awsTask.indicies)
        self.remaining.difference_update(awsTask.indicies)
    def num_completed(self):
        return len(self.completed)
    def num_remaining(self):
        return len(self.remaining)
    def get_indicies(self, size):
        if size > self.num_remaining():
            return list(self.completed)
        indicies = random.sample(self.remaining, size)
        candidates.difference_update(indicies)
        return indicies
    def __eq__(obj):
        return (self.species1 == obj.species1) & (self.species2 == obj.species2) & (self.subfamily == obj.subfamily)

SPECIES = ['Greens', 'Gorillas', 'Orangutans', 'Bushbaby', 'Humans', 'Bonobos', 'Rhesuses', 'Marmosets', 'Squirrels', 'Goldens', 'Mouses', 'Baboons', 'Chimps']
SUBFAMILIES = []