#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

import requests
import os
from dateutil.parser import isoparse
import datetime
from time import sleep
from typing import Tuple
import json

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    ###################################################
    ### Fetch air quality data and push it to Kafka ###
    ###################################################

    # Air Quality Index API token
    with open('token.txt', 'r') as reader:
        TOKEN = reader.read()

    # Monitored cities
    CITIES  = (
        'zurich',
        'geneva',
        'basel',
        'lausanne',
        'bern',
        'winterthur',
        'saint-gallen',
        'lugano',
        'neuchatel'
    )

    # Kafka topic
    TOPIC = 'air_quality_index'

    # Probe the air quality API every REFRESH_TIME seconds
    REFRESH_TIME = 10*60.0

    def get_latest_data(cities: Tuple[str, ...] = CITIES, token: str = TOKEN) -> dict[str, dict[str, str]]:
        '''
        Fetch the latest air quality measurements in the stations listed in ``cities``
        Return the air quality index and time of measurement for each city.
        A token is required to request through the API: https://aqicn.org/data-platform/token/
        '''
        data = {}
        for city in cities:
            url = f'https://api.waqi.info/feed/{city}/?token={token}'
            try:
                response = requests.get(url, timeout = 5)
                response.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                print ("HTTP Error:",errh)
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)
            except requests.exceptions.RequestException as err:
                print ("Unknown error:",err)
            response_json = response.json()
            data[city] = {'aqi': response_json['data']['aqi'], 'time': response_json['data']['time']['iso']}
        return data

    # Fetch the latest airt quality data and push it to the Kafka cluster
    data = get_latest_data()
    for city in data:
        producer.produce(TOPIC, json.dumps(data[city]), city, callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()

    def refresh_data(cities: Tuple[str, ...] = CITIES, token: str = TOKEN) -> None:
        '''
        Update the current air quality index data, stored in global dictionary ``data``, with newer records (if any).
        Push the new records (only) to the Kafka cluster.
        '''
        global data
        global producer

        new_data = get_latest_data(cities, token)
        for city in cities:
            if isoparse(new_data[city]['time']) > isoparse(data[city]['time']):
                data[city] = new_data[city]
                producer.produce(TOPIC, json.dumps(data[city]), city, callback=delivery_callback)
                print(f'Current time: {datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}')
                print(f'New record for {city}: {data[city]}')
        # Block until the messages are sent.
        producer.poll(10000)
        producer.flush()
        return None

# Probe the API for new records every REFRESH_TIME seconds.
# If any, then push those new records to the Kafka cluster.
while True:
    sleep(REFRESH_TIME)
    refresh_data()
