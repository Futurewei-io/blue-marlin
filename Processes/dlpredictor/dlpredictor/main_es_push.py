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

import argparse
import sys
import yaml

from pyspark import SparkContext
from pyspark.sql import HiveContext

from dlpredictor.configutil import *
from dlpredictor.log import *
from dlpredictor import transform


def run (cfg):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    # Load the data frame from Hive.
    table_name = cfg['es_predictions_index']
    command = """select * from {}""".format(table_name)
    df = hive_context.sql(command)

    # Select the columns to push to elasticsearch.
    rdd = df.rdd.map(lambda x: transform.format_data(x, 'ucdoc'))

    # Write the data frame to elasticsearch.
    es_write_conf = {"es.nodes": cfg['es_host'],
                     "es.port": cfg['es_port'],
                     "es.resource": cfg['es_predictions_index']+'/'+cfg['es_predictions_type'],
                     "es.batch.size.bytes": "1000000",
                     "es.batch.size.entries": "100",
                     "es.input.json": "yes",
                     "es.mapping.id": "uckey",
                     "es.nodes.wan.only": "true",
                     "es.write.operation": "upsert"}
    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)

    sc.stop()


if __name__ == '__main__':

    # Get the execution parameters.
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file.
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
            resolve_placeholder(cfg)
            logger_operation.info("Successfully open {}".format(args.config_file))
    except IOError as e:
        logger_operation.error("Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        logger_operation.error("Unexpected error:{}".format(sys.exc_info()[0]))
        raise

    # Run this module.
    run(cfg)

