import pandas as pd
import pm4py
from datetime import datetime, timedelta
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from neo4j import GraphDatabase
import random
import os
from django.conf import settings


def prepare_event_log(file_path):
    dataframe = pd.read_csv(file_path, sep=';')
    start_time = datetime.now()
    dataframe['timestamp'] = [start_time +
                              timedelta(seconds=i * 15 + random.randint(0, 15)) for i in range(len(dataframe))]
    dataframe = pm4py.format_dataframe(
        dataframe, case_id='case_id', activity_key='activity', timestamp_key='timestamp')
    event_log = pm4py.convert_to_event_log(dataframe)
    return event_log


def discover_process_model(event_log):
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(
        event_log)
    return net, initial_marking, final_marking


def visualize_process_model(net, initial_marking, final_marking):
    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    image_path = os.path.join(
        settings.MEDIA_ROOT, "process_model_path.png")
    image_URL = os.path.join(settings.MEDIA_URL, "process_model_path.png")
    print("FROM FUNCTION")
    print(image_path)
    pn_visualizer.save(gviz, image_path)
    return image_URL  # Path to the generated image file


def connect_to_neo4j():
    driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(
        settings.NEO4J_USER, settings.NEO4J_PASSWORD))
    session = driver.session()
    print("\nDatabase Connection")
    if (session):
        print("U just Have a Connection")
    else:
        print("FAILED")
    return session
