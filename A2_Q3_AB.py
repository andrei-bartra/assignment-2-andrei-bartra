# -*- coding: utf-8 -*-
'''
 _______________________________________
 | MACS 30123: Large Scale Computing    |
 | Assignment 2: Kinesis Stream         |
 | Question 3                           |
 | Andrei Bartra                        |
 | May 2021                             |
 |______________________________________|

'''
#  ________________________________________
# |                                        |
# |               1: Settings              |
# |________________________________________|

#Boto3
import boto3
import time

#File transfer
import paramiko
from scp import SCPClient

#Globals
CODE = ['producer.py', 'consumer.py']
KEY = "/home/andrei/aws_keys/MACS LSC.pem"
KEY_NAME = "MACS\ LSC.pem"

#  ________________________________________
# |                                        |
# |           3: Set Up Instances          |
# |________________________________________|


def create_instances():

    session = boto3.Session()

    kinesis = session.client('kinesis')
    ec2 = session.resource('ec2')
    ec2_client = session.client('ec2')

    try:
        response = kinesis.create_stream(StreamName = 'a2q3',
                                        ShardCount = 1)
        print("Streamming service created")
    except:
        print("a2q3 streamming already exists")

    instances = ec2.create_instances(ImageId='ami-0915e09cc7ceee3ab',
                                     MinCount=1,
                                     MaxCount=2,
                                     InstanceType='t2.micro',
                                     KeyName='MACS LSC',
                                     SecurityGroupIds=['sg-0ba8bc79dfd38b4ec'],
                                     SecurityGroups=['a2q3'],
                                     IamInstanceProfile=
                                        {'Name': 'EMR_EC2_DefaultRole'},
                                    )

    waiter = ec2_client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance.id for instance in instances])
    time.sleep(10)
    print("EC2 instances succesfully created")

    #Write file with instances ids
    f = open("ec2_instances.txt", "w")
    f.write("|".join([instance.id for instance in instances]))
    f.close()

    #Get instances DNS

    instance_dns = [instance.public_dns_name 
                    for instance in ec2.instances.all() 
                    if instance.state['Name'] == 'running']
    
    return ec2_client, instances, instance_dns


def ssh_setup(inst_dns):
    ssh_producer, ssh_consumer = paramiko.SSHClient(), paramiko.SSHClient()
    print("waiting for ssh")

    # Initialization of SSH tunnels takes a bit of time; otherwise get connection error on first attempt
    time.sleep(5)

    # Install boto3 on each EC2 instance and Copy our producer/consumer code onto producer/consumer EC2 instances
    instance = 0
    stdin, stdout, stderr = [[None, None] for i in range(3)]
    for ssh in [ssh_producer, ssh_consumer]:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(inst_dns[instance],
                    username = 'ec2-user',
                    key_filename= KEY)
        
        with SCPClient(ssh.get_transport()) as scp:
            scp.put([CODE[instance], "ec2_instances.txt"])
        
            stdin[instance], stdout[instance], stderr[instance] = \
                ssh.exec_command("sudo pip install boto3")

        instance += 1

    # Block until Producer has installed boto3 and testdata, then start running Producer script:
    producer_exit_status = stdout[0].channel.recv_exit_status() 
    if producer_exit_status == 0:
        ssh_producer.exec_command("python %s" % CODE[0])
        print("Producer Instance is Running producer.py\n.........................................")
    else:
        print("Error", producer_exit_status)

    # Close ssh and show connection instructions for manual access to Consumer Instance
    ssh_consumer.close; ssh_producer.close()

    print("Run this!\n sudo ssh -i {} ec2-user@{}".format(KEY_NAME, inst_dns[1]))


def terminate_instances(ec2_client, instances):
    ec2_client.terminate_instances(InstanceIds=[instance.id for instance in instances])

    # Confirm that EC2 instances were terminated:
    waiter = ec2_client.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[instance.id for instance in instances])
    print("EC2 Instances Successfully Terminated")


#  ________________________________________
# |                                        |
# |               4: Wrapper               |
# |________________________________________|

def kinesis_go():
    ec2_client, instances, inst_dns = create_instances()
    time_stamp = time.time()
    print(inst_dns)
    ssh_setup(inst_dns)
    while True:
        if time.time() - time_stamp > 60*5:
            terminate_instances(ec2_client, instances)
            print("YOU FORGOT TO TERMINATE EC2!!!!!")
            break
#  ________________________________________
# |                                        |
# |               5: Parser                |
# |________________________________________|

if __name__ == '__main__':
    kinesis_go()