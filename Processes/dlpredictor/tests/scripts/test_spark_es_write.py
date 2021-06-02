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

"""
This file is to test pushing Spark data frame into Elasticsearch.
Each row in dataframe is a document in Elasticsearch. 
"""
# Reza

import os
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def test_push(sc, es_write_conf):

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


if __name__ == '__main__':

    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--jars ~/elasticsearch-hadoop-6.5.2/dist/elasticsearch-hadoop-6.5.2.jar pyspark-shell'

    sc = SparkContext()

    es_write_conf = {"es.nodes": '10.124.243.233', "es.port": '9200', "es.resource": 'tbr_spark_es_test_02082019/doc',
                     "es.batch.size.entries": '5', "es.input.json": "yes", "es.mapping.id": "doc_id",
                     "es.nodes.wan.only": "true", "es.write.operation": "upsert"}
    test_push(sc=sc, es_write_conf=es_write_conf)
