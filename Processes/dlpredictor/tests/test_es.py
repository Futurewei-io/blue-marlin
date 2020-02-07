
from elasticsearch import Elasticsearch

es_host = "10.124.243.233"
es_port = 9200
es_index = 'reza_test'
es_type = 'doc'

es = Elasticsearch([{'host': es_host, 'port': es_port}])

'''
This part push n document to es
'''
n = 100
for i in range(1, n+1):
    for j in range(0, 10):
        doc_dict = {'i': i, 'j': j, 'ctx_doc': {}}
        es.index(index=es_index, doc_type=es_type, body=doc_dict)
