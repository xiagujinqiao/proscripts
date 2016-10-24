#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka import SimpleClient
from kazoo.client import KazooClient
import time
import json
import socket

ts = int(time.time())
endpoint = socket.gethostname()
counterType = 'GAUGE'
#tags = 'n=kafka'
step = 60

zookeepers="1.1.1.1:2181"
''' zookeeper server '''
kafka="1.1.1.1:9092"
''' kafka server '''


playload=[]
def create_record(metric,value,tags):
   playload.append({
         'metric': metric,
         'endpoint': endpoint,
         'timestamp': int(time.time()),
         'step': 60,
         'value': value,
         'counterType': 'GAUGE',
         'tags': tags
    })

if __name__ == '__main__':
    broker = SimpleClient(kafka)
    lags = {}
    zk = KazooClient(hosts=zookeepers, read_only=True)
    zk.start()
    logsize=0
    groups = zk.get_children("/consumers/")
    for group in groups:
        if zk.exists("/consumers/%s/owners" %(group)):
            topics = zk.get_children("/consumers/%s/owners" %(group))
            for topic in topics:
                logsize =0
                consumer = SimpleConsumer(broker, group, str(topic))
                lag = consumer.pending()
                partitions = zk.get_children("/brokers/topics/%s/partitions" %(topic))
                for partition in partitions:
                    log = "/consumers/%s/offsets/%s/%s" % (group, topic, partition)
                    if zk.exists(log):
                        data, stat = zk.get(log)
                        logsize += int(data)
                latest_offset = logsize - lag
                lags[topic] = lag
                create_record(topic,logsize,"group="+group + ",valueType=logsize")
                create_record(topic,lag,"group="+group + ",valueType=lag")
                create_record(topic,latest_offset,"group="+group + ",valueType=latest_offset")
        else:
            pass
    zk.stop()
    print json.dumps(playload)
