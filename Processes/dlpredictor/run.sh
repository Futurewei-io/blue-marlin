#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Start the predictor
if true
then
    # spark-submit --num-executors 10 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.5.2.jar dlpredictor/main_spark_es.py conf/config.yml '2019-11-03' 's32' '1' 'http://10.193.217.105:8501/v1/models/faezeh:predict'  
    # spark-submit --master yarn --py-files dist/dlpredictor-2.0.0-py2.7.egg,dist/imscommon-2.0.0-py2.7.egg,dist/predictor_dl_model-1.0.0-py2.7.egg --num-executors 3 --executor-cores 3 --jars lib/elasticsearch-hadoop-6.8.0.jar dlpredictor/main_spark_es.py conf/config.yml '2020-02-08' 's32' '1' 'http://10.193.217.105:8501/v1/models/faezeh:predict'
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --py-files $SCRIPTPATH/dist/dlpredictor-1.6.0-py2.7.egg,$SCRIPTPATH/lib/imscommon-2.0.0-py2.7.egg,$SCRIPTPATH/lib/predictor_dl_model-1.6.0-py2.7.egg --jars $SCRIPTPATH/lib/elasticsearch-hadoop-6.8.0.jar $SCRIPTPATH/dlpredictor/main_spark_es.py $SCRIPTPATH/conf/config.yml
fi

# Push the data to elasticsearch
if true
then
    spark-submit --master yarn --num-executors 3 --executor-cores 3 --executor-memory 16G --driver-memory 16G --py-files $SCRIPTPATH/dist/dlpredictor-1.6.0-py2.7.egg,$SCRIPTPATH/lib/imscommon-2.0.0-py2.7.egg --jars $SCRIPTPATH/lib/elasticsearch-hadoop-6.8.0.jar $SCRIPTPATH/dlpredictor/main_es_push.py $SCRIPTPATH/conf/config.yml
fi
