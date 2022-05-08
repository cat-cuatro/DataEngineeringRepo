"""
Author: John Lorenz IV
###
This source code is for an eapplication that is a part of graduate coursework.
This repository will be made private when the class is over.
###
Primary entry point for data production, consumption, and data pulling from the web source
psudataeng.com:8000/getBreadCrumbsData
"""
from fetcher import Fetcher
import sys
import threading
import subprocess

def consumeTopics():
    pass

def consumerScriptCall():
    path = '~/DataEngineeringRepo/DataEngineeringProject/Part2/python/producer.py'
    cmd = ['python3', path, '-f', '~/.confluent/librdkafka.config', '-t', 'test1']
    subprocess.run(' '.join(cmd), shell=True)

def producerScriptCall():
    path = '~/DataEngineeringRepo/DataEngineeringProject/Part2/python/consumer.py'
    cmd = ['python3', path, '-f', '~/.confluent/librdkafka.config', '-t', 'test1']
    subprocess.run(' '.join(cmd), shell=True)

def produceTopics(fname): #unused atm
    t = threading.Thread(target=producerScriptCall, daemon=True)
    t.start()

def main(args):
    dataFetcher = Fetcher()
    if 'consume' in args:
        consumerScriptCall()
    elif 'produce' in args:
        producerScriptCall()
    else:
        fname = dataFetcher.grabBreadCrumbs()
        producerScriptCall()

if __name__ == "__main__":
    main(sys.argv)
