# Copyright 2019, Futurewei Technologies
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


import json
import pickle
from datetime import date
import yaml
import argparse

from elasticsearch import Elasticsearch, helpers


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
