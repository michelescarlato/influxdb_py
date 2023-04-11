#!/usr/bin/python3
import pandas as pd
import json
from influxdb import DataFrameClient

dbhost = 'localhost'
dbport = 8086
dbuser = 'admin'
dbpasswd = 'xxxxx'
dbname = 'schooldb'
protocol = 'line'

# Use only following fields from CSV.
Fields = ['TimeStamp','School','Class','Students','Absent']
# Define tag fields
datatags = ['School','Class']
# Define fixed tags
fixtags = {"Country": "India", "State": "Haryana", "City": "Kurukshetra"}

# Read data from CSV without index and parse 'TimeStamp' as date.
df = pd.read_csv("sample.csv", sep=',', index_col=False, parse_dates=['TimeStamp'], usecols=Fields)

# Set 'TimeStamp' field as index of dataframe
df.set_index('TimeStamp', inplace = True)

print(df.head())

client = DataFrameClient(dbhost, dbport, dbuser, dbpasswd, dbname)
# Write data to "SchoolData" measurement of "schooldb" database.
client.write_points(df,"SchoolData",tags=fixtags,tag_columns=datatags,protocol=protocol)