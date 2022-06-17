#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Save the configuration
if true
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --py-files dist/dlpredictor-2.0.0-py2.7.egg dlpredictor/show_config.py conf/config.yml
fi

# Start the predictor
if true
then
    spark-submit --master yarn --num-executors 15 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-2.0.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-2.0.0-py2.7.egg --jars lib/elasticsearch-hadoop-6.8.0.jar dlpredictor/main_spark_es.py conf/config.yml
fi

# Push the data to elasticsearch
if true
then
    spark-submit --master yarn --num-executors 3 --executor-cores 3 --executor-memory 16G --driver-memory 16G --py-files dist/dlpredictor-2.0.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg --jars lib/elasticsearch-hadoop-6.8.0.jar dlpredictor/main_es_push.py conf/config.yml
fi
