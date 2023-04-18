import logging
import sys
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser
from datetime import datetime
import time
import socket

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
    secs_interval = config['influxdb.parameters']['secs_interval']

    return my_bucket, my_org, my_token, my_url, secs_interval

file_name = sys.argv[1]

bucket, org, token, url, secs_interval = read_conf(file_name)

start = time.time()
log_name = str(datetime.now())
log_name = log_name.replace(" ","_")
print(log_name)
logging.basicConfig(filename="logs/Query_data_"+str(log_name)+"_run.log", level=logging.INFO)

print(socket.gethostname())
logging.info("Result object: "+str(socket.gethostname()))

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)


# Query script
query_api = client.query_api()

query_1_sensor = 'from(bucket: "org_bucket_3")\
|> range(start: -30d)\
|> filter(fn: (r) => r["_measurement"] == "h3o_feet")\
|> filter(fn: (r) => r["_field"] == "m001_abs_good")'

query_all_249_sensors = 'from(bucket: "org_bucket_1")\
|> range(start: -27d)\
|> filter(fn: (r) => r["_measurement"] == "h3o_feet")'

current_query = query_1_sensor

print('Performing query:' + current_query)

result = query_api.query(org=org, query=current_query)
results = []

for table in result:
  for record in table.records:
    results.append((record.get_field(), record.get_value()))

print(results)
print(result)

end = time.time()

elapsed_time = end - start
print("Total time elapsed:")
print(elapsed_time)
logging.info("Total time elapsed: "+str(elapsed_time))
logging.info("Query used: "+str(current_query))
logging.info("Result object: "+str(result))
