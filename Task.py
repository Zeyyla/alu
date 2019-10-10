import pickle
import random 
import Bio

SPECIES = ['Greens', 'Gorillas', 'Orangutans', 'Bushbaby', 'Humans', 'Bonobos', 'Rhesuses', 'Marmosets', 'Squirrels', 'Goldens', 'Mouses', 'Baboons', 'Chimps']
SUBFAMILIES = ["AluJb", "AluJo", "AluJr", "AluJr4", "AluSc", "AluSc5", "AluSc8", "AluSg", "AluSg4", "AluSg7", "AluSp", "AluSq", "AluSq10", "AluSq2", "AluSq4", "AluSx", "AluSx1", "AluSx3", "AluSx4", "AluSz", "AluSz6", "AluY"]

class Task():
    def __init__(self, species1, species2, subfamily, total = None, completed = None, remaining = None, candidates = None):
        self.species1 = species1
        self.species2 = species2
        self.subfamily = subfamily
        self.total = total or len(pickle.load(file=open("data/"+self.species1 + "/" + self.species1+"_"+self.subfamily + ".p", "rb")))
        self.completed = completed or set()
        self.remaining = remaining or {i for i in range(self.total)}
        self.candidates = candidates or {i for i in range(self.total)}

    def filename(self):
        return self.species1 + "_" + self.species2 + "_" + self.subfamily + ".p"

    def update(self, awstask):
        self.completed.update(awstask.indicies)
        self.remaining.difference_update(awstask.indicies)

    def num_completed(self):
        return len(self.completed)

    def num_remaining(self):
        return len(self.remaining)

    def get_aws_task(self, size): 
        """Creating an awsTask object from a Task instance. """
        if len(self.remaining) == 0: 
            return None 
        if size > self.num_remaining(): 
            indicies = list(self.remaining)
        else: 
            indicies = random.sample(self.remaining, size)
        self.candidates.difference_update(indicies)
        return awsTask.fromTask(self, indicies)   

    def aws_to_task(awstask):
        """Return filename extracted from an awsTask object"""
        return awstask.species1 + "_" + awstask.species2 + "_" + awstask.subfamily + ".p"

    def fromTask(task):
        return Task(task.species1, task.species2, task.subfamily, task.total, task.completed, task.remaining, task.candidates)
    
    def __eq__(obj):
        return (self.species1 == obj.species1) & (self.species2 == obj.species2) & (self.subfamily == obj.subfamily)

class awsTask():
    def __init__(self, species1, species2, subfamily, indicies, datas = []):
        self.species1 = species1
        self.species2 = species2
        self.subfamily = subfamily
        self.indicies = indicies
        self.datas = datas

    def fromDict(d):
        return awsTask(d['species1'], d['species2'], d['subfamily'], d['indicies'], d['datas'])
    def fromTask(task, indicies):
        return awsTask(task.species1, task.species2, task.subfamily, indicies)

