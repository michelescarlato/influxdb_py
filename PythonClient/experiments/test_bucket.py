from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import configparser


def read_conf(conf_file):
    config = configparser.ConfigParser()
    # config.read('conf_file.conf')
    config.read(conf_file)
    config.sections()
    my_bucket = config['influxdb.parameters']['bucket']
    my_org = config['influxdb.parameters']['org']
    my_token = config['influxdb.parameters']['token']
    # Store the URL of your InfluxDB instance
    my_url = config['influxdb.parameters']['url']

    return my_bucket, my_org, my_token, my_url

bucket, my_org, my_token, url = read_conf('../conf_file.conf')



#!/usr/bin/env python3
"""Example to read JSON data and send to InfluxDB."""

from dateutil import parser
import json

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

point = Point("weather")
with open("../data/weather.json", "r") as json_file:
    data = json.load(json_file)

    point.tag("station", data["name"])
    point.time(parser.parse(data["updated"]))
    for key, value in data["sensors"].items():
        point.field(key, value)

with InfluxDBClient.from_config_file("config.toml") as client:
    with client.write_api(write_options=SYNCHRONOUS) as writer:
        try:
            writer.write(bucket="my-bucket", record=[point])
        except InfluxDBError as e:
            print(e)