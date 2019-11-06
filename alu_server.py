import boto3
import os
import numpy as np
from Task import Task, awsTask, SPECIES
import json
from sys import exit
import csv
import time
import multiprocessing as mp
from itertools import product, permutations
import pandas as pd
from zipfile import ZipFile



#TODO: check if local file len matches server file len (tags)
#TODO: create a json file on the server that stores structure and size information
def verify_server_data(params):

    ##new process:
    '''
    1) check if zip exists
    2) check if size of zip matches size of server zip
    3) if not, reupload
    '''
    zipPath = os.path.join(params['datapath'], "data.zip")
    jsonPath = os.path.join(params['datapath'], "data_structure.json")

    if not os.path.exists(zipPath):
        print("Please compress data files into 'data.zip'")
        exit()

    if not os.path.exists(jsonPath):
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
        json.dump(td, open("data_structure.json", "w+"))
    
    s3 = boto3.resource('s3', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'])
    aludata = s3.Bucket('aludata')
    #TODO: add metedata for last_updated, and check if the local file has eben updated since then before uploading
    aludata.upload_file("data_structure.json", "data_structure.json")
    print("Server JSON is (already) up-to-date.")

    #check local zipfile size
    #check server zipfile size
    #update if mismatch
    serverZip = aludata.Object("data.zip")
    # serverZip.
    with ZipFile(zipPath, "r") as zip_file:
        localSize = zip_file.file_size
    if serverSize != localSize:
        print("Server data does not match local data. Reuploading now.")
    with tqdm(total=dataFile.content_length, unit='B', unit_scale=True, desc="data.zip") as t:
        #TODO: chack params
        aludata.upload_file("data.zip", path.join(dataPath, "data.zip"), Callback=t.update)


    
#TODO error handling
def process_responses(params, responses):
    if "Messages" not in responses.keys():
        return
    sqs = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'])
    for msg in responses['Messages']:
        awstask = awsTask.fromDict(json.loads(msg['Body']))
        print("Recieved task: {} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies)))
        task = json.load(open("tasks/" + Task.aws_to_task(awstask).replace(".p", ".json"), "r"))
        task = Task(task["species1"], task["species2"], task["subfamily"], task["total"], task["completed"], task["remaining"], task["candidates"])
        names = np.concatenate([["ind"]] + [["c"+str(i), "s"+str(i), "e"+str(i)] for i in range(6)])
        df = pd.read_csv(resultFile, names=names)
        completed = set(df["ind"])
        datas = [awstask.datas[i] for i in range(len(awstask.datas)) if awstask.datas[i][0] not in completed]
        wr = csv.writer(open("results/" + task.filename() + ".csv", "a+", newline=''), quoting=csv.QUOTE_ALL)
        wr.writerows(datas)
        task.update(awstask)
        json.dump(task, open("tasks/" + task.filename(), "w"))
        print("Task {} - {} - {} is {}%% done".format(task.species1, task.species2, task.subfamily, task.num_completed()/task.total))
        nextTask = generate_aws_task(awstask)
        if nextTask is not None:
            nextMsg = json.dumps(nextTask.__dict__)
            response = sqs.send_message(QueueUrl=params["task_url"], MessageBody=nextMsg)
            print("Pushed task: {} - {} - {} with {} indicies".format(nextTask.species1, nextTask.species2, nextTask.subfamily, len(nextTask.indicies)))
            print("\tMessage ID: {}".format(response["MessageId"]))
        else:
            print("Task: {} - {} - {} is complete".format(task.species1, task.species2, task.subfamily))
        sqs.delete_message(QueueUrl=params['results_url'], ReceiptHandle=msg['ReceiptHandle'])

def generate_starter_aws_tasks(params, size=50):
    sqs = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'])
    attributes = sqs.get_queue_attributes(QueueUrl=params['task_url'], AttributeNames=["ApproximateNumberOfMessages"])
    approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
    print("We have {} tasks.".format(approxQueueSize))
    if approxQueueSize < 1000:
        print("That's not enough - adding more tasks.")
        for taskFile in os.listdir("tasks/"):
            if ".json" not in taskFile:
                continue
            t = json.load(open("tasks/" + taskFile, "r"))
            t = Task(t['species1'], t['species2'], t['subfamily'], t['total'], t['completed'], t['remaining'], t['candidates'],)
            awstask = t.get_aws_task(size)
            if awstask is not None:
                msg = json.dumps(awstask.__dict__)
                sqs.send_message(QueueUrl=params['task_url'], MessageBody=msg)
        print("Added a bunch of tasks.")        

def generate_aws_task(prevAWSTask):
    task = json.load(open("tasks/" + Task.aws_to_task(prevAWSTask), "r"))
    task = Task(task["species1"], task["species2"], task["subfamily"], task["total"], task["completed"], task["remaining"], task["candidates"])
    size = len(prevAWSTask.indicies)
    return task.get_aws_task(size)

def get_len(file):
    species1 = file.split("_")[0] 
    return (file, len(json.load(file=open("data/"+species1 + "/" + file, "r"))))

def verify_task_consistency(taskFile):
    if ".json" not in taskFile:
        return
    task = json.load(open("tasks/" + taskFile, "r"))
    task = Task(task["species1"], task["species2"], task["subfamily"], task["total"], task["completed"], task["remaining"], task["candidates"])
    resultFile = "results/" + task.filename() + ".csv"
    if os.path.exists(resultFile):
        names = np.concatenate([["ind"]] + [["c"+str(i), "s"+str(i), "e"+str(i)] for i in range(6)])
        df = pd.read_csv(resultFile, names=names)
        completed = set(df["ind"])
        if completed != task.completed:
            print("Error with task: {}".format(task.filename()[:-2]))
            task.completed = completed
            task.remaining = {i for i in range(task.total)}
            task.candidates = {i for i in range(task.total)}
            task.remaining.difference_update(completed)
            task.candidates.difference_update(completed)
            json.dump(task, open("tasks/" + taskFile, "w"))
    else:
        if task.completed != set():
            task.completed = set()
            task.remaining = {i for i in range(task.total)}
            json.dump(task, open("tasks/" + taskFile, "w"))

def verify_internal_consistency():
    print("Verifying internal task consistency...")
    with mp.Pool(mp.cpu_count() - 1) as p:
        [_ for _ in p.map(verify_task_consistency, os.listdir("tasks/"))]
    print("Task consistency verified.")

if __name__ == "__main__":
    credentialFile = "awsServerCredentials.json"
    params = json.load(open(credentialFile, "r"))
    # verify_server_data(params)
    verify_internal_consistency()
    generate_starter_aws_tasks(params)
    sqs = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'], aws_secret_access_key=params['aws_secret_access_key'])
    while True:
        attributes = sqs.get_queue_attributes(QueueUrl=params['results_url'], AttributeNames=["ApproximateNumberOfMessages"])
        approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
        print("We have {} completed tasks waiting.".format(approxQueueSize))
        if approxQueueSize < 100:
            print("Waiting for more responses...")
            #TODO: estimate time till a decent number of tasks and then sleep (some weighted rate calculation)
            time.sleep(30)
        else:
            for _ in range(approxQueueSize//10):
                response = sqs.receive_message(QueueUrl=params["results_url"], MaxNumberOfMessages=10, WaitTimeSeconds=20)
                process_responses(params, response)
    