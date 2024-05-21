from kafka import KafkaProducer, KafkaConsumer

import json
import requests


def create_kafka_producer():

    producer = KafkaProducer(bootstrap_servers=['kafka:29092'],
                             value_serializer=lambda x: json.dumps(x)
                             .encode('utf-8'))
    return producer


def create_kafka_consumer(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['kafka:29092'],
                             auto_offset_reset='earliest',
                             value_deserializer=lambda x: json
                             .loads(x.decode('utf-8')))
    return consumer


def get_data():

    res = requests.get('https://randomuser.me/api')
    res = res.json()
    res = res['res'][0]

    return res


def format_data(res):

    data = {}
    data["full_name"] = f"{res['name']['title']}. {res['name']['first']} \
                          {res['name']['last']}"
    data["gender"] = res["gender"]
    data["location"] = f"{res['location']['street']['number']}, \
                         {res['location']['street']['name']}"
    data["city"] = res['location']['city']
    data["country"] = res['location']['country']
    data["postcode"] = int(res['location']['postcode'])
    data["latitude"] = float(res['location']['coordinates']['latitude'])
    data["longitude"] = float(res['location']['coordinates']['longitude'])
    data["email"] = res["email"]

    return data


def stream_data():
    topic = 'user_data'
    producer = create_kafka_producer()
    for i in range(5):
        res = get_data()
        data = format_data(res)
        producer.send(topic, value=data)
        producer.flush()
