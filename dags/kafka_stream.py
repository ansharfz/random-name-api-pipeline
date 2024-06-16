from kafka import KafkaProducer

import json
import requests


def create_kafka_producer():
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                             value_serializer=lambda x: json.dumps(x)
                             .encode('utf-8'))
    return producer


def get_data():

    res = requests.get('https://randomuser.me/api')
    res = res.json()
    res = res['results'][0]

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
    data["postcode"] = res['location']['postcode']
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


stream_data()
