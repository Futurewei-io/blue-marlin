import logging
import json
from elasticsearch import Elasticsearch

import sys
import os

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('ims-logger')


class ESClient:

    def __init__(self, host, port, es_index, es_type):
        if host == '':
            logger.error("ES hosts is None .")
            raise ValueError('hosts is None .')
        if port == '':
            logger.error("ES port is None .")
            raise ValueError('port is None .')

        self.es_index = es_index
        self.es_type = es_type
        self.es = Elasticsearch([{'host': host, 'port': port}])
        if self.es.ping():
            logger.info("Successfully connect to ES.")
        else:
            logger.error("Failed to build connect to ES.")
            raise ValueError('Fail to build connect to ES.')

    def __put(self, uckey, dict=dict):
        try:
            dict_res = self.es.index(
                index=self.es_index, doc_type=self.es_type, id=uckey, body=dict)
        except Exception as e:
            logger.error(
                "Failed to put a new doc to ES index: %s,error message is : %s" % (self.es_index, e))
            raise ValueError('Fail to put a new doc to ES .')
        return dict_res

    def __get(self, uckey):
        try:
            dict_res = self.es.get(index=self.es_index,
                                   doc_type=self.es_type, id=uckey)
        except Exception as e:
            logger.error(
                "Failed to get doc from ES by key, ES index: %s ,error message is : %s" % (self.es_index, e))
            raise ValueError('Fail to get doc from ES by key.')
        return dict_res

    def put(self, doc_id, ucdoc):
        json_doc = json.dumps(ucdoc, default=lambda x: x.__dict__)
        return self.__put(doc_id, json_doc)

    def index(self, id, doc):
        return self.es.index(index=self.es_index, doc_type=self.es_type, id=id, body=doc)

    def does_exist(self, uckey):
        try:
            return self.es.exists(index=self.es_index, doc_type=self.es_type, id=uckey)
        except Exception as e:
            logger.error(
                "Failed to judge whether a doc exists in ES index: %s ,error message is : %s" % (self.es_index, e))
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
        try:
            res = self.es.indices.refresh(index=self.es_index)
        except Exception as e:
            logger.error(
                "Failed to refresh ES index: %s ,error message is : %s" % (self.es_index, e))
            return False

    def partial_update(self, uckey, key, value):
        to_be_updated = {key: value}
        doc = {'doc': to_be_updated}
        str_to_be_updated = json.dumps(doc, default=lambda x: x.__dict__)
        try:
            res = self.es.update(
                index=self.es_index, doc_type=self.es_type, id=uckey, body=str_to_be_updated)
        except Exception as e:
            logger.error(
                "Failed to update a doc in ES index: %s ,error message is : %s" % (self.es_index, e))
        return res

    def update_doc_by_query(self, id, body_str):
        try:
            res = self.es.update(index=self.es_index,
                                 doc_type=self.es_type, id=id, body=body_str)
        except Exception as e:
            logger.error(
                "Failed to update a doc in ES index: %s ,error message is : %s" % (self.es_index, e))
        return res

    def update_by_query(self, body):
        try:
            res = self.es.update_by_query(
                index=self.es_index, doc_type=self.es_type, body=body)
        except Exception as e:
            logger.error(
                "Failed to update by query in ES index: %s ,error message is : %s" % (self.es_index, e))
        return res

    def search(self, body):
        res = self.es.search(index=self.es_index, body=body)
        return [hit["_source"] for hit in res['hits']['hits']]


if __name__ == '__main__':
    es_host = '10.193.217.111'
    es_port = '9200'

    es = ESClient(es_host, es_port, 'model_stats', 'stat')
    body = {
        "query": {"bool": {"must": [
            {"match": {
                "model.name": "s32"
            }},
            {"match": {
                "model.version": 1
            }}
        ]}}
    }
    print(es.search(body))
