import boto3
import os
import numpy as np
from Task import Task, awsTask, SPECIES
import json
import pickle
from sys import exit
import csv
import time


#TODO: check if local file len matches server file len (tags)
#TODO: create a json file on the server that stores structure and size information
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
    if "Messages" not in responses.keys():
        return
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    for msg in response['Messages']:
        awstask = awsTask(json.loads(msg['Body']))
        print("Recieved task: {} - {} - {} with {} indicies".format(awstask.species1, awstask.species2, awstask.subfamily, len(awstask.indicies)))
        with open("tasks/" + Task.aws_to_task(awstask), "rb") as taskFile:
            task = pickle.load(taskFile)
        with open("results/" + task.filename() + ".csv", "a+", newline='') as csvFile:
            wr = csv.writer(csvFile, quoting=csv.QUOTE_ALL)
            wr.writerows(awstask.datas)
        task.update(awstask)
        with open("tasks/" + task.filename(), "wb") as taskFile:
            pickle.dump(task, taskFile)
        sqs.delete_message(QueueUrl=credentials['results_url'], ReceiptHandle=msg['ReceiptHandle'])
        nextTask = generate_aws_task(awstask)
        if nextTask is not None:
            msg = pickle.dumps(nextTask)
            sqs.send_message(QueueUrl=credentials["task_url"], MessageBody=msg)

#TODO: stopping conditions
def generate_starter_aws_tasks(credentials, size=50):
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    attributes = sqs.get_queue_attributes(QueueUrl=credentials['task_url'], AttributeNames=["ApproximateNumberOfMessages"])
    approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
    print("We have {} tasks.".format(approxQueueSize))
    if approxQueueSize < 1000:
        print("That's not enough - adding more tasks.")
        tasks = []
        for taskName in os.listdir("tasks/"):
            with open("tasks/" + taskName, "rb") as taskFile:
                tasks.append(pickle.load(taskFile))
        np.random.shuffle(tasks)
        for t in tasks:
            awstask = t.get_aws_task(size)
            if awstask is not None:
                msg = json.dumps(awstask.__dict__)
                sqs.send_message(QueueUrl=credentials['task_url'], MessageBody=msg)
        print("Added a bunch of tasks.")        

#TODO: stopping conditions
def generate_aws_task(prevAWSTask):
    with open("tasks/" + Task.aws_to_task(prevAWSTask), "rb") as taskFile:
        task = pickle.load(taskFile)
    size = len(prevAWSTask.indicies)
    nextTask = task.get_aws_task(size)
    if nextTask is None:
        return None
    return nextTask.get_aws_task(size)

if __name__ == "__main__":
    credentialFile = "awsServerCredentials.json"
    with open(credentialFile, "r") as f:
        credentials = json.load(f)
    # verify_server_data(credentials)
    generate_starter_aws_tasks(credentials)
    sqs = boto3.client('sqs', aws_access_key_id=credentials['aws_access_key_id'], aws_secret_access_key=credentials['aws_secret_access_key'])
    print("Waiting for responses...")
    response = sqs.receive_message(QueueUrl=credentials["results_url"], MaxNumberOfMessages=10, WaitTimeSeconds=20)
    process_responses(credentials, response)
    attributes = sqs.get_queue_attributes(QueueUrl=credentials['task_url'], AttributeNames=["ApproximateNumberOfMessages"])
    # approxQueueSize = int(attributes['Attributes']['ApproximateNumberOfMessages'])
    # print("We have {} tasks.".format(approxQueueSize))
    # import pdb; pdb.set_trace()

    
    
