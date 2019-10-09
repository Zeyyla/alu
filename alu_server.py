import boto3
import os
import numpy as np
from Task import SPECIES
import json
from awsTask import awsTask
import pickle
from sys import exit
import csv


#TODO: check if local file len matches server file len (tags)
def verify_server_data(credentials):
    s3 = boto3.resource('s3', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    aludata = s3.Bucket('aludata')
    filelist = {file for species_list in [os.listdir("data/" + s) for s in SPECIES] for file in species_list}
    uploaded = {file.key for file in aludata.objects.all()}
    
    #remove files that have already been uploaded to the server from filelist
    filelist.difference_update(uploaded)

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
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    for msg in response['Messages']:
        awstask = pickle.loads(msg['Body'])
        #print stuff
        task = pickle.load("tasks/" + Task.aws_to_task(awstask))
        with open("results/" + task.filename() + ".csv", "a+", newline='') as f:
            wr = csv.writer(file, quoting=csv.QUOTE_ALL)
            wr.writerows(datas)
        task.update(awstask)
        sqs.delete_message(QueueUrl=credentials['results_url'], ReceiptHandle=msg['ReceiptHandle'])
        nextTask = generate_aws_task(awstask)
        msg = pickle.dumps(nextTask)
        sqs.send_message(QueueUrl=credentials["task_url"], MessageBody=msg)

#TODO: stopping conditions
def generate_starter_aws_tasks(credentials, size=50):
    attributes = sqs.get_queue_attributes(QueueUrl=credentials['task_url'], AttributeNames=["ApproximateNumberOfMessages"])
    approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
    print("We have {} tasks.".format(approxQueueSize))
    if approxQueueSize < 1000:
        print("That's not enough - adding more tasks.")
        tasks = []
        for taskFile in os.listdir("tasks/"):
            with open("tasks/" + task) as taskFile:
                tasks.append(pickle.load("tasks/" + taskFile))
        np.random.shuffle(tasks)
        for t in tasks:
            awstask = t.get_aws_task(size)
            if awstask is not None:
                msg = pickle.dumps(awstask)
                sqs.send_message(QueueUrl=credentials['results_url'], MessageBody=msg)
        print("Added a bunch of tasks.")        

#TODO: stopping conditions
def generate_aws_task(prevAWSTask):
    task = pickle.load("tasks/" + Task.aws_to_task(prevAWSTask))
    size = len(prevAWSTask.indicies)
    nextTask = task.get_aws_task(size)
    if nextTask is None:
        return
    return nextTask.get_aws_task(size)

if __name__ == "__main__":
    credentialFile = "awsServerCredentials.json"
    with open(credentialFile, "r") as f:
        credentials = json.load(f)
    verify_server_data(credentials)
    generate_starter_aws_tasks(credentials)
