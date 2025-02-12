# import InfluxDB_ConfigurationInstance
import logging
import sys
import influxdb_client
from influxdb_client import WritePrecision, InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS#, BATCHING
import pandas as pd
import os
import fnmatch
from datetime import datetime
from datetime import timedelta
import time
import socket
from mira_utils import read_conf,convert_seconds


def write_db(my_bucket, my_org, my_token, my_url, data):
    client = influxdb_client.InfluxDBClient(
        url=my_url,
        token=my_token,
        org=my_org
    )
    # Write script
    write_api = client.write_api(write_options=SYNCHRONOUS)

    #p = influxdb_client.Point("my_measurement2").tag("location", "Rome").field("temperature", 25.1)
    # how to add more fields?
    p = influxdb_client.Point("machine_test").tag("test", "pandas")\
        .field("value2", data[1]) \
        .time(data[0], WritePrecision.MS)


    write_api.write(bucket=my_bucket, org=org, record=p)
    print("Row: " + str(data[0])+ " time, " +str(data[1]) + " inserted ")
    global points_inserted_count
    points_inserted_count = points_inserted_count + 1
    print (points_inserted_count)
    return


def write_csv_data_to_db_few_values(csv_file, bucket, org, token, url, new_epoch):
    #data = pd.read_csv(csv_file, sep=' |,') #delimiter=',')
    data = pd.read_csv(csv_file, sep=',')  # delimiter=',')
    # points by points data    -->take 1,12 columns --> create time series gg, sec, a random value,
    for t in data.itertuples():
        value = t[11]
        # increasing timestamp by 90 secs
        # Fields cannot be added to Pandas tuples - need a conversion to list
        l = list(t)
        # adding timestamp to the first list element
        l[0] = new_epoch
        # rollback list to tuple
        t = tuple(l)
        data_db = [new_epoch, value]
        write_db(bucket, org, token, url, data_db)
    return new_epoch


def write_csv_data_to_db_250_values(csv_file, bucket, org, token, url, new_epoch, secs_interval, currentTime):
    data = pd.read_csv(csv_file, sep=',')
    # take the first 250 values
    first_column = data.iloc[:,0]
    second_n_column = data.iloc[:, 1:249].astype(float)
    third_column = data.loc[:,'m001_abs_good']
    row_index = 0
    if new_epoch > currentTime:
        return new_epoch
    for t in first_column:
        # increasing timestamp by x secs
        new_epoch = new_epoch + timedelta(seconds=int(secs_interval))
        first_column.iat[row_index] = new_epoch
        row_index = row_index + 1
    result = pd.concat([first_column, second_n_column,third_column], axis=1)
    write_db_bulk(bucket, org, token, url, result)
    return new_epoch

def write_db_bulk(my_bucket, my_org, my_token, my_url, data):
    with InfluxDBClient(url=my_url, token=my_token, org=my_org) as _client:
        with _client.write_api(write_options=WriteOptions(batch_size=500,
                                                          flush_interval=10_000,
                                                          jitter_interval=2_000,
                                                          retry_interval=5_000,
                                                          max_retries=5,
                                                          max_retry_delay=30_000,#max_close_wait=300_000,
                                                          exponential_base=2)) as _write_client:
            """
            Write Pandas DataFrame
            """
            data = data.set_index(['timems'])
            _write_client.write(my_bucket, my_org, record=data, data_frame_measurement_name='h3o_feet',
                                data_frame_tag_columns=['timems'])

    global points_inserted_count
    points_inserted_count = points_inserted_count + len(data.index)
    print("after bulk write - points inserted so far:"+str(points_inserted_count))
    return


def load_data(bucket, org, token, url, csv_dir, epoch, secs_interval):
    currentTime = datetime.utcnow()
    print(currentTime)
    while epoch < currentTime:
        print(epoch)
        dir = 0
        while dir <= 2:
            file = 0
            count = len(fnmatch.filter(os.listdir(csv_dir + str(dir)), '*.*'))
            while file < count:
                csv_filename = csv_dir + str(dir) + '/' + str(file) + ".csv"
                #epoch = write_csv_data_to_db_few_values(csv_filename, bucket, org, token, url, epoch)
                if epoch > currentTime:
                    return epoch
                epoch = write_csv_data_to_db_250_values(csv_filename, bucket, org, token, url, epoch, secs_interval, currentTime)
                file = file + 1
            dir = dir + 1
    logging.info('Last data inserted at time _ epoch inside load data: ' + str(epoch))
    print(epoch)
    return epoch

def get_fields_name(csv_path):
    data = pd.read_csv(csv_path, sep=',')
    fields_name = data.columns.values.tolist()
    fields_name_250 = fields_name[0:249]
    print(fields_name_250)
    return fields_name_250

log_name = str(datetime.now())
log_name = log_name.replace(" ","_")
hostname = str(socket.gethostname())
print(log_name)
logging.basicConfig(filename="logs/Insert_data_"+hostname+"_"+str(log_name)+"_run.log", level=logging.INFO)

print(socket.gethostname())
logging.info("Result object: "+hostname)

file_name = sys.argv[1]
# start script temporizer
start = time.time()

points_inserted_count = 0
epoch = datetime.utcnow() - timedelta(days=30)

bucket, org, token, url, secs_interval = read_conf.read_conf(file_name)
csv_dir = '../CSV_machine_data/'

# get the fields to use for the points
fields_name = get_fields_name("../CSV_machine_data/0/1.csv")

# load the CSVs and get the last recorded time inserted
time_end = load_data(bucket, org, token, url, csv_dir, epoch, secs_interval)
print('Last data inserted at time: '+str(time_end))
logging.info('Last data inserted at time: '+str(time_end))
print("Total number of points inserted:" + str(points_inserted_count))
logging.info("Total number of points inserted:" + str(points_inserted_count))

end = time.time()

elapsed_time = end - start
minutes = convert_seconds.convert_seconds(elapsed_time)
print("Total time elapsed:")
print(minutes)
logging.info("Total time elapsed (hh:mm:ss): "+str(minutes))
logging.info("Seconds interval: "+str(secs_interval))
logging.info("Bucket used: "+str(bucket))
