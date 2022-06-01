"""
Author: John Lorenz IV
###
This source code is for an eapplication that is a part of graduate coursework.
This repository will be made private when the class is over.
###
Primary entry point for data production, consumption, and data pulling from the web source
psudataeng.com:8000/getBreadCrumbsData
"""
import sys
import threading
import subprocess

def consumerScriptCall():
    path = '~/DataEngineeringRepo/Assignment7/python/consumer.py'
    cmd = ['python3', path, '-f', '~/.confluent/librdkafka.config', '-t', 'test1']
    subprocess.run(' '.join(cmd), shell=True)

def producerScriptCall():
    path = '~/DataEngineeringRepo/Assignment7/python/producer.py'
    cmd = ['python3', path, '-f', '~/.confluent/librdkafka.config', '-t', 'test1']
    subprocess.run(' '.join(cmd), shell=True)

def main(args):
    if 'consume' in args:
        print('Consuming some data!')
        consumerScriptCall()
    elif 'produce' in args:
        print('Producing some data!')
        producerScriptCall()

if __name__ == "__main__":
    main(sys.argv)
