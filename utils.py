# File of various useful functions 

#get the frequencies for each subfamily in a genome
#ex: get_frequencies("humans")
class Task():
    def __init__(self,task):
        self.species1 = task[0][0]
        self.species2 = task[0][1]
        self.subfamily = task[1]
        self.total = len(pickle.load(file=open("data/"+self.species1 + "/" + self.species1+"_"+self.subfamily + ".p", "rb")))
        self.completed = 0
    def filename(self):
        return self.species1 + "_" + self.species2 + "_" + self.subfamily + ".p"
    def update(self,amount):
        self.completed += amount
    def finished(self):
        return self.completed == self.total
    def remaining(self):
        return self.total - self.completed
    def __eq__(self, obj):
        return (self.species1 == obj.species1) & (self.species2 == obj.species2) & (self.subfamily == obj.subfamily)
def create_tasks(tasks):
#warning: only run once, will reset progress for tasks
    for task in tasks:
        t = Task(task)
        pickle.dump(t, open("tasks/"+t.filename(), "wb"))
#     task_list = [Task(task) for task in tasks]
#     pickle.dump(obj=task_list, file=open("progress.p", "wb"))
def create_pickles(genomes):
    d = {}
    for g in genomes:
        records = SeqIO.parse("data/" + g + ".fasta", "fasta")
        for a in alu:
            d[a]=[]
        for r in records:
            subfamily = r.id.split("_")[2]
            if subfamily in d.keys():
                d[subfamily].append(r)
        mkdir("data/" + g)
        for sub, seqs in d.items():
            path = "data/" + g + "/" + g + "_" + sub + ".p"
            pickle.dump(seqs, open(path, "wb"))
        
        
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

# #reverse lookup for sequence description given species, single chromosome, start, and end 
# def description_lookup(species, subfamily, chromosome, start, end): 
#     sequences = pickle.load(file=open("data/"+ species + "/" + species +"_" + subfamily + ".p", "rb"))
#     description = chromosome+":"+str(start) + "-" + str(end) 
#     for s in sequences: 
#         desc = s.description.split(" ")[1][6:]
#         if desc == description: 
#             return s.seq 

#reverse lookup for sequence given lists of chromosomes, start, and end
def description_lookup(species, subfamily, chromosomes, starts, ends): 
    assert len(chromosomes) == len(starts) and len(starts) == len(ends) and len(chromosomes) == len(ends)
    lookup = {} 
    result = []
    sequences = pickle.load(file=open("data/"+ species + "/" + species +"_" + subfamily + ".p", "rb"))
    for s in sequences: 
        desc = s.description.split(" ")[1][6:]
        lookup[desc] = s.seq
    descriptions = [str(chromosomes[i]) + ":" + str(starts[i]) + "-" + str(ends[i]) for i in range(len(chromosomes))] 
    for d in descriptions: 
        result.append(lookup[d])
    return result         