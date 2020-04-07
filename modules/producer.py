from cgi import log

from kafka import KafkaProducer
import json
import requests
import csv

from kafka.errors import KafkaError

key1 = "api"
key2 = "csv"
bootstrap_servers = ['localhost:9092']
topic_name = 'topic1'


def data_from_csv(producer):
    with open('resources/AAPL.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            line = json.dups(row)
            future = producer.send(topic_name, key=key2, value=line)

            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                log.exception()


def data_from_apis(producer):
    api_url = "https://financialmodelingprep.com/api/v3/company/profile/AAPL"
    response = requests.get(api_url)
    if response.status_code >= 500:
        print('[!] [{0}] Server Error'.format(response.status_code))
    elif response.status_code == 404:
        print('[!] [{0}] URL not found: [{1}]'.format(response.status_code, api_url))
    elif response.status_code == 401:
        print('[!] [{0}] Authentication Failed'.format(response.status_code))
    elif response.status_code == 400:
        print('[!] [{0}] Bad Request'.format(response.status_code))
    elif response.status_code >= 300:
        print('[!] [{0}] Unexpected Redirect'.format(response.status_code))
    elif response.status_code == 200:
        res = response.json()
        future = producer.send(topic_name, key=key1, value=res)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:

            log.exception()
    else:
        print('[?] Unexpected Error: [HTTP {0}]: Content: {1}'.format(response.status_code, response.content))


def data_from_others():
    pass


def start_producer(message):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda m: m.encode('utf-8'),
                             key_serializer=str.encode)
    data_from_apis(producer)
    data_from_csv(producer)
