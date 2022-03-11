import yaml
import argparse
import datetime
from kazoo.client import KazooClient


def run(cfg):

    cfg_zk = cfg["zookeeper"]
    zk = KazooClient(hosts=cfg_zk["zookeeper_hosts"])
    zk.start()

    es_index_prefix, stat = zk.get(cfg_zk["es_index_prefix"])
    es_index = es_index_prefix + '-' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")



    print("{" +'\n' +
          "output for ctr_score_generator.py is: "+ '\n\n'+
          "es_index: "+ es_index + '\n'+ "}" )



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Performance Forecasting: CTR Score Generator")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    run(cfg=cfg)
