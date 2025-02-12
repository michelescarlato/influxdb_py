import logging
import sys
import influxdb_client
from datetime import datetime
import time
import socket
from mira_utils import read_conf,convert_seconds


file_name = sys.argv[1]

bucket, org, token, url, secs_interval = read_conf.read_conf(file_name)


log_name = str(datetime.now())
log_name = log_name.replace(" ","_")
hostname = str(socket.gethostname())
print(log_name)
logging.basicConfig(filename="logs/Query_data_"+hostname+"_"+str(log_name)+"_run.log", level=logging.INFO)

print(hostname)
logging.info("Result object: "+hostname)

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)


# Query script
query_api = client.query_api()

field_name = "m001_abs_good"
measurement_name = "h3o_feet"

query_1_sensor = f'from(bucket: "{bucket}")\
|> range(start: -30d)\
|> filter(fn: (r) => r["_measurement"] == "{measurement_name}")\
|> filter(fn: (r) => r["_field"] == "{field_name}")'

query_1_sensor_fixed_range = f'from(bucket: "{bucket}")\
|> range(start: 2023-03-19T06:47:30.000Z, stop: 2023-04-18T07:47:13.000Z)\
|> filter(fn: (r) => r["_measurement"] == "{measurement_name}")\
|> filter(fn: (r) => r["_field"] == "{field_name}")'

query_1_sensor_fixed_range_1_day = f'from(bucket: "{bucket}")\
|> range(start: 2023-03-22T00:00:00.000Z, stop: 2023-03-23T00:00:00.000Z)\
|> filter(fn: (r) => r["_measurement"] == "{measurement_name}")\
|> filter(fn: (r) => r["_field"] == "{field_name}")'

query_all_249_sensors = f'from(bucket: "{bucket}")\
|> range(start: -27d)\
|> filter(fn: (r) => r["_measurement"] == "{measurement_name}")'

current_query = query_1_sensor_fixed_range_1_day

print('Performing query:' + current_query)
start = time.time()
result = query_api.query(org=org, query=current_query)
end = time.time()
results = []

for table in result:
  for record in table.records:
    results.append((record.get_field(), record.get_value()))

print(results)
print(result)



elapsed_time = end - start
minutes = convert_seconds.convert_seconds(elapsed_time)
print("Total time elapsed:")
print(minutes)
logging.info("Total time elapsed (hh:mm:ss): "+str(minutes))
logging.info("Query used: "+str(current_query))
logging.info("Bucket used: "+str(bucket))
#logging.info("Result object: "+str(result))
