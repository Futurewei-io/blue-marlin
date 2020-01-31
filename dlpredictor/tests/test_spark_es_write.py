import os
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--jars /home/reza/eshadoop/elasticsearch-hadoop-6.5.2/dist/elasticsearch-hadoop-6.5.2.jar pyspark-shell'

sc = SparkContext(appName="AllCountries")

es_write_conf = {"es.nodes": '10.124.243.233', "es.port": '9200', "es.resource": 'tbr_spark_es_test_02082019/doc',
                 "es.batch.size.entries": '5', "es.input.json": "yes", "es.mapping.id": "doc_id",
                 "es.nodes.wan.only": "true", "es.write.operation": "upsert"}

data = [{"days": {'key13': 'value13'}, 'doc_id': "1230112"},
        {"days": {'key22': 'value22'}, 'doc_id': "4560112"}]
rdd = sc.parallelize(data)


def format_data(x):
    return (x['doc_id'], json.dumps(x))


rdd = rdd.map(lambda x: format_data(x))

rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_write_conf)
