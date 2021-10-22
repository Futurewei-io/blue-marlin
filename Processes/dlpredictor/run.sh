#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Save the configuration
if true
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --py-files $SCRIPTPATH/dist/dlpredictor-1.6.0-py2.7.egg $SCRIPTPATH/dlpredictor/show_config.py $SCRIPTPATH/conf/config.yml
fi

# Build ipl_dist_map table AND unique_origianl_uckey
if true
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-1.6.0-py2.7.egg --conf spark.driver.maxResultSize=5G dlpredictor/main_build_ipl_dist.py conf/config.yml
fi

# Start the predictor
if true
then
    # spark-submit --num-executors 10 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.5.2.jar dlpredictor/main_spark_es.py conf/config.yml '2019-11-03' 's32' '1' 'http://10.193.217.105:8501/v1/models/faezeh:predict'  
    # spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-1.6.0-py2.7.egg --conf spark.driver.maxResultSize=5G dlpredictor/main_spark_es.py conf/config.yml
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files $SCRIPTPATH/dist/dlpredictor-1.6.0-py2.7.egg,$SCRIPTPATH/lib/imscommon-2.0.0-py2.7.egg,$SCRIPTPATH/lib/predictor_dl_model-1.6.0-py2.7.egg --jars $SCRIPTPATH/lib/elasticsearch-hadoop-6.8.0.jar $SCRIPTPATH/dlpredictor/main_spark_es.py $SCRIPTPATH/conf/config.yml
fi

# Push the data to elasticsearch
if true
then
    # spark-submit --master yarn --num-executors 3 --executor-cores 3 --executor-memory 16G --driver-memory 16G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg --jars lib/elasticsearch-hadoop-6.8.0.jar dlpredictor/main_es_push.py conf/config.yml
    spark-submit --master yarn --num-executors 3 --executor-cores 3 --executor-memory 16G --driver-memory 16G --py-files $SCRIPTPATH/dist/dlpredictor-1.6.0-py2.7.egg,$SCRIPTPATH/lib/imscommon-2.0.0-py2.7.egg --jars $SCRIPTPATH/lib/elasticsearch-hadoop-6.8.0.jar $SCRIPTPATH/dlpredictor/main_es_push.py $SCRIPTPATH/conf/config.yml
fi
