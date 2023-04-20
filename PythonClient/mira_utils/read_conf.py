import configparser

def read_conf(conf_file):
    config = configparser.ConfigParser()
    config.read(conf_file)
    config.sections()
    my_bucket = config['influxdb.parameters']['bucket']
    my_org = config['influxdb.parameters']['org']
    my_token = config['influxdb.parameters']['token']
    # Store the URL of your InfluxDB instance
    my_url = config['influxdb.parameters']['url']
    secs_interval = config['influxdb.parameters']['secs_interval']

    return my_bucket, my_org, my_token, my_url, secs_interval