'''
 _______________________________________
 | MACS 30123: Large Scale Computing    |
 | Assignment 2: Kinesis Stream         |
 | Question 3 - Consumer code           |
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
import time
import json

#Kinesis connection
kinesis = boto3.client('kinesis', region_name='us-east-1')

shard_it = kinesis.get_shard_iterator(StreamName = "a2q3",
                                     ShardId = 'shardId-000000000000',
                                     ShardIteratorType = 'LATEST'
                                     )["ShardIterator"]


#  ________________________________________
# |                                        |
# |         2: Consummer Operation         |
# |________________________________________|


def cond_behavior(cond, subject, message, kill=True):
    if cond:
        sns = boto3.client('sns', region_name='us-east-1')
        response = sns.create_topic(Name='a2q3')

        subscr = sns.subscribe(
                        TopicArn = response['TopicArn'],
                        Protocol='email',
                        Endpoint='andrei.bartra@gmail.com')
        input("User suscribed?: ")

        # Send message
        sns.publish(TopicArn = response['TopicArn'],
        Message = message,
        Subject = subject)

        input("Message Received?: ")
        # Delete topic
        sns.delete_topic(TopicArn=response['TopicArn'])
        
        if kill:
            # Delete Kinesis stream 
            try:
                response = kinesis.delete_stream(StreamName='a2q3')
            except kinesis.exceptions.ResourceNotFoundException:
                pass
            # Confirm that Kinesis Stream was deleted:
            waiter = kinesis.get_waiter('stream_not_exists')
            waiter.wait(StreamName='a2q3')
            print("Kinesis Stream Successfully Deleted")

        return True
    else:
        return False
            
        
def consumer(shard, verbose=True, delay=0.2):
    i = 0
    while True:
        out = kinesis.get_records(ShardIterator = shard, Limit = 1)
        for o in out['Records']:
            jdat = json.loads(o['Data'])
            i += 1

        if i != 0 and verbose:
            print("{} price is {} at {}".format(jdat['TICKER'], jdat['PRICE'], jdat['EVENT_TIME']))
            print("\n")

        mail = {'subject': "{} stock alert!".format(jdat['TICKER']),
                'message': "{} stock price is ${} at {}".format(jdat['TICKER'], jdat['PRICE'], jdat['EVENT_TIME'])}

        triggered = cond_behavior(jdat['PRICE'] < 3, mail['subject'], mail['message'], kill=True)
        if triggered:
            print("Finished")
            break
        shard = out['NextShardIterator']
        time.sleep(delay)


#  ________________________________________
# |                                        |
# |                3: Parser               |
# |________________________________________|


if __name__ == '__main__':
    consumer(shard_it)