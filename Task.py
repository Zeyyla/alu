import pickle

class Task():
    def __init__(self, task, subsection = None):
        self.species1 = task[0][0]
        self.species2 = task[0][1]
        self.subfamily = task[1]
        self.total = len(pickle.load(file=open("data/"+self.species1 + "/" + self.species1+"_"+self.subfamily + ".p", "rb")))
        self.completed = 0
        self.subsection = subsection
    def filename(self):
        if self.subsection is None:
            return self.species1 + "_" + self.species2 + "_" + self.subfamily + ".p"
        else:
            return self.species1 + "_" + self.species2 + "_" + self.subfamily + "_" + str(self.subsection) + ".p"
    def update(self,amount):
        self.completed += amount
    def finished(self):
        return self.completed == self.total
    def remaining(self):
        return self.total - self.completed
    def __eq__(obj):
        return (self.species1 == obj.species1) & (self.species2 == obj.species2) & (self.subfamily == obj.subfamily)