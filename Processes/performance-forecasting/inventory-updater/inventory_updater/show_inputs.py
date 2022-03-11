import datetime
import yaml
import argparse
from kazoo.client import KazooClient
from pyspark import SparkContext
from pyspark.sql import HiveContext


def run(cfg, hive_context):

    # load zookeeper paths and start kazooClient to load configs.
    cfg_zk = cfg["zookeeper"]
    zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
    zk.start()

    # load gucdocs and keywords from the hive.
    gucdocs_table, stat = zk.get(cfg_zk["gucdocs_loaded_table"])
    keywords_table, stat = zk.get(cfg_zk["keywords_table"])

    # load all the es configs for saving gucdocs to es.
    es_host, stat = zk.get(cfg_zk["es_host"])
    es_port, stat = zk.get(cfg_zk["es_port"])
    es_index_prefix, stat = zk.get(cfg_zk["es_index_prefix"])
    es_index = es_index_prefix + '-' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    es_type, stat = zk.get(cfg_zk["es_type"])
    es_starting_day, stat = zk.get(cfg_zk["es_starting_day"])
    es_ending_day, stat = zk.get(cfg_zk["es_ending_day"])
    es_starting_hour, stat = zk.get(cfg_zk["es_starting_hour"])
    es_ending_hour, stat = zk.get(cfg_zk["es_ending_hour"])
    ims_inventory_serving_url_daily, stat = zk.get(cfg_zk["ims_inventory_serving_url_daily"])

    command = "SELECT * FROM {}"
    df_gucdocs = hive_context.sql(command.format(gucdocs_table))
    df_gucdocs_schema = df_gucdocs.schema

    df_keyword = hive_context.sql(command.format(keywords_table))
    df_keyword_schema = df_keyword.schema

    print("{" + '\n' +
          "inputs for inventory_updater.py is: " + '\n\n' +
          "table 1: " + gucdocs_table + '\n\n' +
          "table 1 schema: " + str(df_gucdocs_schema) + '\n\n' +
          "table 2: " + keywords_table + '\n\n' +
          "table 2 scheama: " + str(df_keyword_schema) + '\n\n' +
          "ims_inventory_serving_ur_daily: " + ims_inventory_serving_url_daily + '\n\n' +
          "es_host: " + es_host + '\n\n' +
          "es_port: " + es_port + '\n' "}")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performance Forecasting: Inventory Updater")
    parser.add_argument('config_file')
    args = parser.parse_args()
    # Load config file
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(cfg=cfg, hive_context=hive_context)
