import logging
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser
from datetime import datetime
import time
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


bucket, org, token, url, secs_interval = read_conf('conf_file.conf')
start = time.time()
log_name = str(datetime.now())
log_name = log_name.replace(" ","_")
print(log_name)
logging.basicConfig(filename="logs/Query_data_"+str(log_name)+"_run.log", level=logging.INFO)

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)


# Query script
query_api = client.query_api()

query = 'from(bucket: "org_bucket_11")\
|> range(start: -30d)\
|> filter(fn: (r) => r["_measurement"] == "h3o_feet")\
|> filter(fn: (r) => r["_field"] == "m001s1_productivity")'

result = query_api.query(org=org, query=query)
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
logging.info("Query used: "+str(query))
logging.info("Result object: "+str(result))