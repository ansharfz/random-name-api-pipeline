from kafka import KafkaProducer

import json
import requests


def create_kafka_producer():
    """
    Creates a Kafka producer instance configured to serialize values as JSON.

    Returns
    -------
    producer : kafka.KafkaProducer
        A Kafka producer instance that can be used to send
        messages to a Kafka topic.

    """
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                             value_serializer=lambda x: json.dumps(x)
                             .encode('utf-8'))
    return producer


def get_data():
    """
    Fetches a random user data from the randomuser.me API and
    returns the results.

    Returns
    -------
    dict
        A dictionary containing the random user data.
    """

    res = requests.get('https://randomuser.me/api')
    res = res.json()
    res = res['results'][0]

    return res


def format_data(res):
    """
    Formats the data retrieved from the randomuser.me API into a dictionary
    to be processed to be written to a Kafka topic.

    Parameters
    ----------
    res : dict
        A dictionary containing the random user data.

    Returns
    -------
    dict
        A dictionary containing the formatted user data, including
        full name, gender, location, city, country, postcode
        latitude, longitude, and email.
    """

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
    """
    Streams data to a Kafka topic.

    This function connects to a Kafka producer, retrieves data from a source,
    formats the data, and sends it to a Kafka topic.

    Returns
    -------
        None
    """

    topic = 'user_data'
    producer = create_kafka_producer()
    for i in range(5):
        res = get_data()
        data = format_data(res)
        producer.send(topic, value=data)
        producer.flush()


stream_data()
