import boto3
import os
import numpy as np
from Task import SPECIES
import json
from awsTask import awsTask
import pickle
from sys import exit

# sqs get size of queue (?) sqs.get_queue_attributes(AttributeNames=["ApproximateNumberOfMessages"])
#on recieve, post new message for same task with updated params

#TODO: check if local file len matches server file len (tags)
def verify_server_data(credentialFile):
    with open(credentialFile, "r") as f:
        keys = json.load(f)
    s3 = boto3.resource('s3', aws_access_key_id=keys['aws_access_key_id'], aws_secret_access_key=keys['aws_secret_access_key'])
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
def process_message(msg):
    t = pickle.loads(msg['body'])
    #create row of csv with index and relevant data
    #write row to appropriate file
    #delete message if no error

#TODO: stopping conditions
def generate_starter_aws_tasks(tasks, size=50):
    #if ApproximateNumberOFMessages < 100 & there are tasks remaining:
    for t in tasks:
        aT = t.get_AWSTask(size)
            if aT is not None:
                #push to message board

#TODO: stopping conditions
def generate_aws_task(prevAWSTask = None, size=50):
    nextTask = None #actual task object
    if prevTask is None:
        nextTask = #do we keep all tasks in memory? how do we pass them around? functools.partial? getTask function?
    else:
        nextTask = #extracted from awsTask
    awsTask = nextTask.get_AWSTask(size) #write get_AWSTask, return None if complete

    #push AWS task to message board

if __name__ == "__main__":
    verify_server_data("awsServerCredentials.json")
    