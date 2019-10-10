import boto3
import os
import numpy as np
from Task import Task, awsTask, SPECIES
import json
import pickle
from sys import exit
import csv
import time
import multiprocessing as mp
from itertools import product, permutations
import pandas as pd


#TODO: check if local file len matches server file len (tags)
#TODO: create a json file on the server that stores structure and size information
def verify_server_data(credentials):
    td = {}
    filelist = {file for species_list in [os.listdir("data/" + s) for s in SPECIES] for file in species_list}
    with mp.Pool(mp.cpu_count() - 1) as p:
        sizes = p.map(get_len, filelist)
    for s in SPECIES:
        td[s] = {"names" : [], "lens" : []}    
    for s in sizes:
        species1 = s[0].split("_")[0]
        td[species1]["names"].append(s[0])
        td[species1]["lens"].append(s[1])
    with open("data_structure.json", "w+") as df:
        json.dump(td, df)
    
    s3 = boto3.resource('s3', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    aludata = s3.Bucket('aludata')
    filelist = {file for species_list in [os.listdir("data/" + s) for s in SPECIES] for file in species_list}
    uploaded = {file.key for file in aludata.objects.all()}
    
    #remove files that have already been uploaded to the server from filelist
    filelist.difference_update(uploaded)

    aludata.upload_file("data_structure.json", "data_structure.json")

    for file in filelist:
        print("Missing: " + file)
        path = "data/" + file.split("_")[0] + "/" + file
        aludata.upload_file(path, file)

    filelist = {file for species_list in [os.listdir("data/" + s) for s in SPECIES] for file in species_list}
    uploaded = {file.key for file in aludata.objects.all()}

    if filelist == uploaded:
        print("All server data has been verified")
    else:
        print("Error in verifying server data")
        exit()

    
#TODO error handling
def process_responses(credentials, responses):
    if "Messages" not in responses.keys():
        return
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    for msg in responses['Messages']:
        awstask = awsTask.fromDict(json.loads(msg['Body']))
        print("Recieved task: {} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies)))
        with open("tasks/" + Task.aws_to_task(awstask), "rb") as taskFile:
            task = pickle.load(taskFile)
        with open("results/" + task.filename() + ".csv", "a+", newline='') as csvFile:
            df = pd.read_csv(csvFile, names = ["ind", "c1", "s1", "e1", "c2", "s2", "e2", "k"])
            completed = set(df["ind"])
            datas = [awstask.datas[i] for i in range(len(awstask.datas)) if awstask.datas[i][0] not in completed]
            wr = csv.writer(csvFile, quoting=csv.QUOTE_ALL)
            wr.writerows(datas)
        task.update(awstask)
        with open("tasks/" + task.filename(), "wb") as taskFile:
            pickle.dump(task, taskFile)
        print("Task {} - {} - {} is {}%% done".format(task.species1, task.species2, task.subfamily, task.num_completed()/task.total))
        nextTask = generate_aws_task(awstask)
        if nextTask is not None:
            nextMsg = json.dumps(nextTask.__dict__)
            response = sqs.send_message(QueueUrl=credentials["task_url"], MessageBody=nextMsg)
            print("Pushed task: {} - {} - {} with {} indicies".format(nextTask.species1, nextTask.species2, nextTask.subfamily, len(nextTask.indicies)))
            print("\tMessage ID: {}".format(response["MessageId"]))
        else:
            print("Task: {} - {} - {} is complete".format(task.species1, task.species2, task.subfamily))
        sqs.delete_message(QueueUrl=credentials['results_url'], ReceiptHandle=msg['ReceiptHandle'])

def generate_starter_aws_tasks(credentials, size=50):
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    attributes = sqs.get_queue_attributes(QueueUrl=credentials['task_url'], AttributeNames=["ApproximateNumberOfMessages"])
    approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
    print("We have {} tasks.".format(approxQueueSize))
    if approxQueueSize < 1000:
        print("That's not enough - adding more tasks.")
        for taskFile in os.listdir("tasks/"):
            t = pickle.load(open("tasks/" + taskFile))
            awstask = t.get_aws_task(size)
            if awstask is not None:
                msg = json.dumps(awstask.__dict__)
                sqs.send_message(QueueUrl=credentials['task_url'], MessageBody=msg)
        print("Added a bunch of tasks.")        

def generate_aws_task(prevAWSTask):
    with open("tasks/" + Task.aws_to_task(prevAWSTask), "rb") as taskFile:
        task = pickle.load(taskFile)
    size = len(prevAWSTask.indicies)
    return task.get_aws_task(size)

def get_len(file):
    species1 = file.split("_")[0] 
    return (file, len(pickle.load(file=open("data/"+species1 + "/" + file, "rb"))))

def verify_task_consistency(taskFile):
    task = pickle.load(open("tasks/" + taskFile, "rb"))
    resultFile = "results/" + task.filename() + ".csv"
    if os.path.exists(resultFile):
        df = pd.read_csv(resultFile, names=["ind", "c1", "s1", "e1", "c2", "s2", "e2", "k"])
        completed = set(df["ind"])
        if completed != task.completed:
            print("Error with task: {}".format(task.filename()[:-2]))
            task.completed = completed
            task.remaining = {i for i in range(task.total)}
            task.candidates = {i for i in range(task.total)}
            task.remaining.difference_update(completed)
            task.candidates.difference_update(completed)
            pickle.dump(task, open("tasks/" + taskFile, "wb"))
    else:
        if task.completed != set():
            task.completed = set()
            task.remaining = {i for i in range(task.total)}
            pickle.dump(task, open("tasks/" + taskFile, "wb"))



def verify_internal_consistency():
    print("Verifying internal task consistency...")
    with mp.Pool(mp.cpu_count() - 1) as p:
        [_ for _ in p.map(verify_task_consistency, os.listdir("tasks/"))]
    print("Task consistency verified.")


if __name__ == "__main__":
    credentialFile = "awsServerCredentials.json"
    with open(credentialFile, "r") as f:
        credentials = json.load(f)
    # verify_server_data(credentials)
    # verify_internal_consistency()
    generate_starter_aws_tasks(credentials)
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    while True:
        attributes = sqs.get_queue_attributes(QueueUrl=credentials['results_url'], AttributeNames=["ApproximateNumberOfMessages"])
        approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
        print("We have {} completed tasks waiting.".format(approxQueueSize))
        if approxQueueSize < 100:
            print("Waiting for more responses...")
            time.sleep(30)
        else:
            for _ in range(approxQueueSize//10):
                response = sqs.receive_message(QueueUrl=credentials["results_url"], MaxNumberOfMessages=10, WaitTimeSeconds=20)
                process_responses(credentials, response)
    