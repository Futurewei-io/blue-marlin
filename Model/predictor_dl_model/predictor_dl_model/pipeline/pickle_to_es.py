from elasticsearch import Elasticsearch, helpers
import json
import pickle
from datetime import date
import yaml
import argparse

today = date.today()


def stat_generator(cfg):
    result = {'_index': es_index, '_type': es_type, '_source': {'date': today, 'model': {
        'name': cfg['trainer']['name'], 'version': cfg['save_model']['model_version'],'duration':cfg['tfrecorder_reader']['duration'], 'train_window': cfg['save_model']['train_window'], 'predict_window': cfg['trainer']['predict_window']}, 'stats': pk['stats']}}
    return result


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')

    parser.add_argument('config_file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    with open(cfg['pipeline']['tf_statistics_path'], 'rb') as handle:
        pk = pickle.load(handle)

    es_host = cfg['elastic_search']['es_host']
    es_port = cfg['elastic_search']['es_port']
    es_index = cfg['elastic_search']['es_index']
    es_type = cfg['elastic_search']['es_type']

    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    action = [stat_generator(cfg)]
    helpers.bulk(es, action)
