
import os
import json

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, create_map, sum as sum_agg, struct, map_concat
from pyspark.sql.types import IntegerType, StringType, ArrayType, MapType, FloatType, StructField, StructType
from pyspark.sql import HiveContext
from pyspark.sql.window import Window


def format_data(x, field_name):
    """
    the return is (<doc_id>,<json_doc>)
    doc_id has to be inside <json_doc>
    """
    _ucdoc = {'a': str(x['age']),
              'g': str(x['gender']),
              't': str(x['net_type']),
              'm': str(x['media']),
              'r': str(x['region_id'])}
    _doc = {'ucdoc': _ucdoc, 'uckey': str(x['uckey']), 'kws': x['kws']}
    return (str(x['uckey']), json.dumps(_doc))


def push_to_es(sc, es_write_conf):
    """
    On the following table
    Aggregates on uckey
    Pushes to ES
    +----------------------------+-------------+--------+-------------------+------------------+---+------+--------------+--------+------+---+---------+
    |uckey                       |keyword_index|keyword |keyword_click_count|keyword_show_count|ctr|media |media_category|net_type|gender|age|region_id|
    +----------------------------+-------------+--------+-------------------+------------------+---+------+--------------+--------+------+---+---------+
    |native,AI assistant,2G,0,5,1|25           |shopping|0                  |1                 |0.0|native|AI assistant  |2G      |0     |5  |1        |
    +----------------------------+-------------+--------+-------------------+------------------+---+------+--------------+--------+------+---+---------+

    """
    din_trainready_with_kw_ctr = 'din_testing_ucdocs_09112020_gdin'
    hive_context = HiveContext(sc)
    df = hive_context.sql('select * from {}'.format(din_trainready_with_kw_ctr))
    df = df.withColumn('kw_ctr', create_map([col('keyword'), col('ctr')]))
    uckey_window = Window.partitionBy('uckey')
    df = df.withColumn('_kws_0', collect_list('kw_ctr').over(uckey_window))
    df = df.dropDuplicates(['uckey'])
    df = df.withColumn('kws', udf(lambda x: dict(kv for _map in x for kv in _map.items()), MapType(StringType(), StringType()))('_kws_0'))
    rdd = df.rdd.map(lambda x: format_data(x, 'ucdoc'))
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)


if __name__ == '__main__':

    """
    spark-submit --jars lib/elasticsearch-hadoop-6.8.0.jar scripts/hive_to_es.py
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.8.0.jar scripts/hive_to_es.py
    """

    cfg = {
        'es_host': '10.213.37.41',
        'es_port': '9200',
        'es_index': 'pf_gucdoc_09112020',
        'es_type': 'doc'
    }

    # "es.write.operation": "upsert" adds key:value to the existing doc, it does not remove the already existed keys
    es_write_conf = {"es.nodes": cfg['es_host'],
                     "es.port": cfg['es_port'],
                     "es.resource": cfg['es_index']+'/'+cfg['es_type'],
                     "es.batch.size.bytes": "1000000",
                     "es.batch.size.entries": "100",
                     "es.input.json": "yes",
                     "es.mapping.id": "uckey",
                     "es.nodes.wan.only": "true",
                     "es.write.operation": "upsert"}

    sc = SparkContext()
    sc.setLogLevel('info')

    push_to_es(sc=sc, es_write_conf=es_write_conf)

    sc.stop()
