import os
import sys
import time
#from cassandra.cluster import Cluster
#import Settings
#from Configurations import PlantConfiguration




confList = list()
confNames = list()


def load_configurations(conf_file):
    global confList
    global confNames
    confList = list()
    confNames = list()
    confFiles = list()
    if Settings.ACQ_SEPARATED:
        f = open(os.path.join("BalancerConfigs", conf_file + ".txt"), "r")
        tmpFiles = f.readlines()
        f.close()
        for file in tmpFiles:
            confFiles.append(file.replace("\n", ""))
    cluster = None
    session = None
    while cluster is None or session is None:
        try:
            cluster = Cluster(Settings.CLUSTER_IPs)
            session = cluster.connect(Settings.CASSANDRA_KEYSPACE)
            if cluster and session:
                configs = session.execute("SELECT * FROM macchina")
                session.shutdown()
                cluster.shutdown()
                for conf in configs:
                    if Settings.ACQ_SEPARATED:
                        if conf.conf_path in confFiles:
                            confList.append(PlantConfiguration.PlantConfiguration(conf.monica_id, conf.conf_path, conf.enabled, conf.gateway))
                            confNames.append(conf.monica_id)
                    else:
                        if conf.enabled:
                            confList.append(PlantConfiguration.PlantConfiguration(conf.monica_id, conf.conf_path, conf.enabled, conf.gateway))
                            confNames.append(conf.monica_id)
        except Exception as e:
            print(e)
        sys.stdout.flush()
        time.sleep(2)


# creazione e/o modifica delle tabelle nel DB
def create_tables():
    cluster = None
    session = None
    try:
        cluster = Cluster(Settings.CLUSTER_IPs)
        session = cluster.connect(Settings.CASSANDRA_KEYSPACE)
    except Exception as e:
        print(e)
    if cluster is None or session is None:
        print("Could not create tables because DB connection failed!!!")
        return
        # diagnostic
    try:
        query = "CREATE TABLE IF NOT EXISTS plant_status (plant text, month text, day int, timems int, mqtt boolean, bus boolean, " \
                "PRIMARY KEY ((plant, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
        session.execute(query)
    except Exception as e:
        print("Table creation failed for diagnostic")
    for c in confList:
        if len(c.dbConf) > 0:
            print("creating configuration ", c.name)
            try:
                # alarms...
                try:
                    query = "CREATE TABLE IF NOT EXISTS " + c.name + "_alarms (var_id text, month text, day int, timems int, value boolean, " \
                                                                        "PRIMARY KEY ((var_id, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                                                                        "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
                    session.execute(query)
                except Exception as e:
                    print("Table creation failed for alarms")
                # warnings...
                try:
                    query = "CREATE TABLE IF NOT EXISTS " + c.name + "_warnings (var_id text, month text, day int, timems int, value boolean, " \
                                                                        "PRIMARY KEY ((var_id, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                                                                        "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
                    session.execute(query)
                except Exception as e:
                    print("Table creation failed for warnings")
                # variables...
                try:
                    query = "CREATE TABLE IF NOT EXISTS " + c.name + "_vars_int (var_id text, month text, day int, timems int, value int, " \
                                                                        "PRIMARY KEY ((var_id, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                                                                        "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
                    session.execute(query)
                except Exception as e:
                    print("Table creation failed for int variables")
                try:
                    query = "CREATE TABLE IF NOT EXISTS " + c.name + "_vars_float (var_id text, month text, day int, timems int, value float, " \
                                                                        "PRIMARY KEY ((var_id, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                                                                        "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
                    session.execute(query)
                except Exception as e:
                    print("Table creation failed for float variables")
                try:
                    query = "CREATE TABLE IF NOT EXISTS " + c.name + "_vars_text (var_id text, month text, day int, timems int, value text, " \
                                                                        "PRIMARY KEY ((var_id, month), day, timems) ) WITH CLUSTERING ORDER BY (day ASC, timems ASC) " \
                                                                        "AND COMPACTION = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};"
                    session.execute(query)
                except Exception as e:
                    print("Table creation failed for text variables")
            except:
                print("error during table creation on ", c.name)
    session.execute("UPDATE setting SET value='1' WHERE property='generated_DB';")
    session.shutdown()
    cluster.shutdown()


def check_configuration_updated():
    cluster = None
    session = None
    try:
        cluster = Cluster(Settings.CLUSTER_IPs)
        session = cluster.connect(Settings.CASSANDRA_KEYSPACE)
    except Exception as e:
        print(e)
    if cluster is None or session is None:
        print("Could not check configuration because DB connection failed!!!")
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()
        return False
    try:
        isGen = session.execute("SELECT value FROM setting WHERE property = 'generated_DB';")
        if isGen[0].value == '0':
            session.shutdown()
            cluster.shutdown()
            return True
    except:
        pass
    session.shutdown()
    cluster.shutdown()
    return False

