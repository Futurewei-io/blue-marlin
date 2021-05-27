#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
 
#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import requests
from elasticsearch import Elasticsearch
import yaml
import argparse

class ESClient:

    def __init__(self, host, port, es_index, es_type):
        self.es_index = es_index
        self.es_type = es_type
        self.es = Elasticsearch([{'host': host, 'port': port}])

    def __put(self, uckey, dict=dict):
        dict_res = self.es.index(index=self.es_index, doc_type=self.es_type, id=uckey, body=dict)
        return dict_res

    def __get(self, uckey):
        dict_res = self.es.get(index=self.es_index, doc_type=self.es_type, id=uckey)
        return dict_res

    def put(self, doc_id, ucdoc):
        json_doc = json.dumps(ucdoc, default=lambda x: x.__dict__)
        return self.__put(doc_id, json_doc)

    def index(self, id, doc):
        return self.es.index(index=self.es_index, doc_type=self.es_type, id=id, body=doc)

    def does_exist(self, uckey):
        try:
            return self.es.exists(index=self.es_index, doc_type=self.es_type, id=uckey)
        except:
            return False

    def get(self, uckey):
        dict_res = self.__get(uckey)
        return dict_res

    def get_source(self, uckey):
        dict_res = self.__get(uckey)
        if '_source' in dict_res:
            return dict_res['_source']
        return None

    def refresh_indices(self):
        self.es.indices.refresh(index=self.es_index)

    def get_last_update(self, uckey):
        res = self.__get(uckey)
        js = res['_source']
        if 'lastUpdate' in js.keys():
            return js['lastUpdate']
        return None

    def partial_update(self, uckey, key, value):
        to_be_updated = {key: value}
        doc = {'doc': to_be_updated}
        str_to_be_updated = json.dumps(doc, default=lambda x: x.__dict__)
        res = self.es.update(index=self.es_index, doc_type=self.es_type, id=uckey, body=str_to_be_updated)
        return res

    def update_doc_by_query(self, id, body_str):
        res = self.es.update(index=self.es_index, doc_type=self.es_type, id=id, body=body_str)
        return res

    def post_date(self, uckey, date_str):
        to_be_updated = {'date': date_str}
        return self.partial_update(uckey, 'lastUpdate', to_be_updated)

    def post_last_update(self, uckey, ucday):
        return self.partial_update(uckey, 'lastUpdate', ucday)

    def update_by_query(self, body):
        return self.es.update_by_query(index=self.es_index, doc_type=self.es_type, body=body)

def save_model(model_name,model_version):
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    cfg = cfg['elasticsearch']
    es = ESClient(cfg['host'],cfg['port'],cfg['index'],cfg['type'])
