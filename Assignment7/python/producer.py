#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from re import L
from confluent_kafka import Producer, KafkaError
import json
import pandas as pd
import ccloud_lib
import sys
import os
sys.path.append('/home/lorenz2/DataEngineeringRepo/Assignment7')

def manualLoader():
    dirs = os.listdir('../../..')
    files = []
#    for f in dirs:
#        if 'ascii' in f:
#            to_read = '/home/lorenz2/'+str(f)
#            files.append(to_read)

    f = open('/home/lorenz2/2022-04-30-ascii')
    data = json.load(f)
    files.append('/home/lorenz2/2022-04-29-ascii')
    return files

def produceData(data, type_retrieved, producer, ackfunc):
    for i in range(len(data)):
        record_key = type_retrieved
        data[i].update({'count': i})
        record_value = json.dumps(data[i])
        producer.produce(topic, key=record_key, value=record_value, on_delivery=ackfunc)
        producer.poll(0)

def convertToPacketList(stop_event_dataframe):
    packets = []
    for i in range(0, len(stop_event_dataframe.index)):
        packet = stop_event_dataframe.iloc[[i]].to_dict(orient='index') # god I hate dataframes sometimes..
        packets.append(packet[i]) # this is an obtuse way of stripping the indexed key from the dictionary that gets returned
    return packets

if __name__ == '__main__':
    ######### if manually loading
    files = ['/home/lorenz2/2022-05-16-ascii']#, '/home/lorenz2/2022-05-17-ascii', '/home/lorenz2/2022-05-18-ascii']
    #files = manualLoader()
    #########
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)
    delivered_records = 0
    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
    ## redo
    if files:
        for f in files:
            current = open(f)
            data = json.load(current)
            current.close()
            print('loading..', current)
            for i in range(len(data)):
                record_key = "breadcrumbs"
                data[i].update({'count': i})
                record_value = json.dumps(data[i])
                producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
                producer.poll(0)

    producer.flush()
    print(sys.argv)

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
