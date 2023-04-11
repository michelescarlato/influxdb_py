import InfluxDB_ConfigurationInstance
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser

config = configparser.ConfigParser()
config.read('conf_file.conf')
config.sections()

bucket = config['influxdb.parameters']['bucket']
org = config['influxdb.parameters']['org']
token = config['influxdb.parameters']['token']
# Store the URL of your InfluxDB instance
url = config['influxdb.parameters']['url']


client = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)

# Write script
write_api = client.write_api(write_options=SYNCHRONOUS)

p = influxdb_client.Point("my_measurement2").tag("location", "Rome").field("temperature", 25.1)
write_api.write(bucket=bucket, org=org, record=p)
