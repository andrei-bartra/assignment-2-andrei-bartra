# -*- coding: utf-8 -*-
'''
 _______________________________________
 | MACS 30123: Large Scale Computing    |
 | Assignment 2: Kinesis Stream         |
 | Question 3 - Producer code           |
 | Andrei Bartra                        |
 | May 2021                             |
 |______________________________________|

'''
#  ________________________________________
# |                                        |
# |               1: Settings              |
# |________________________________________|

#Libraries
import boto3
import random
import datetime
import json

#Kinesis connectiom
kinesis = boto3.client('kinesis', region_name='us-east-1')


#  ________________________________________
# |                                        |
# |            2: Data Generator           |
# |________________________________________|

def getReferrer():
    data = {}
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['EVENT_TIME'] = str_now
    data['TICKER'] = 'AAPL'
    price = random.random() * 100 # Assume price is in USD
    data['PRICE'] = round(price, 2)
    return data

while True:
    data = json.dumps(getReferrer())
    kinesis.put_record(StreamName = "a2q3",
                       Data = data,
                       PartitionKey = "partitionkey")