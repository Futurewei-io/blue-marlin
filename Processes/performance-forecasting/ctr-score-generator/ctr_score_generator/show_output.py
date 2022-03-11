import yaml
import argparse
from kazoo.client import KazooClient
from pyspark import SparkContext
from pyspark.sql import HiveContext

def run(cfg, hive_context):

    cfg_zk = cfg["zookeeper"]
    zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
    zk.start()

    # load dataframes for score generator with zookeeper configs on the cluster
    gucdocs_table, stat = zk.get(cfg_zk["gucdocs_loaded_table"])


    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))
    df_gucdocs_schema = df_gucdocs.schema


    print("{" +'\n' +
          "output for ctr_score_generator.py is: "+ '\n\n'+
          "table 1: "+ gucdocs_table + '\n\n'+
          "table 1 schema: " + str(df_gucdocs_schema)+'\n' "}"
           )



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Performance Forecasting: CTR Score Generator")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)


    run(cfg=cfg, hive_context= hive_context)
