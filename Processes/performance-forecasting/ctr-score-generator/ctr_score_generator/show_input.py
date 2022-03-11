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
    gucdocs_table, stat = zk.get(cfg_zk["gucdocs_table"])
    keywords_table, stat = zk.get(cfg_zk["keywords_table"])
    din_serving_url, stat = zk.get(cfg_zk["din_model_tf_serving_url"])
    din_model_length, stat = zk.get(cfg_zk["din_model_length"])

    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))
    df_gucdocs_schema = df_gucdocs.schema

    df_keywords_table = hive_context.sql(command.format(keywords_table))
    df_keywords_table_schema = df_keywords_table.schema

    print("{" +'\n' +
          "inputs for ctr_score_generator.py is: "+ '\n\n'+
          "table 1: "+ gucdocs_table + '\n\n'+
          "table 1 schema: " + str(df_gucdocs_schema)+'\n\n' +
          "table 2: "+ keywords_table +  '\n\n' +
          "table 2 scheama: "+ str(df_keywords_table_schema) + '\n\n' +
          "url address: "+ din_serving_url + '\n\n' +
          "length: "+ din_model_length + '\n' "}")



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
