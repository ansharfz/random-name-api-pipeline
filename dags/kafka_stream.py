# from time import time
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer

import json
import requests


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 4, 13, 00),
}


def create_kafka_producer():

    producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
                             value_serializer=lambda x: json.dumps(x)
                             .encode('utf-8'))
    return producer


def create_kafka_consumer(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             value_deserializer=lambda x: json
                             .loads(x.decode('utf-8')))
    return consumer


def get_data():

    res = requests.get('https://randomuser.me/api')
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):

    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} " \
                      f"{location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, " \
                      f"{location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    topic = 'user_data'
    producer = create_kafka_producer()
    for i in range(5):
        res = get_data()
        data = format_data(res)
        producer.send(topic, value=data)
        producer.flush()


with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
            task_id='stream_data_from_api',
            python_callable=stream_data,
            dag=dag)

streaming_task
