# producer.py
from kafka import KafkaProducer
import json
import time
import random
from pm4py.objects.log.importer.xes import importer as xes_importer

log = xes_importer.apply("new_log.xes")
log = log[:5]
print(log)