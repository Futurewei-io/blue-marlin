#!/bin/bash

#Start the predictor
if true
then
    spark-submit --num-executors 10 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.5.2.jar dlpredictor/main_spark_es.py conf/config.yml '2019-11-03' 's32' '1' 'http://10.193.217.105:8501/v1/models/faezeh:predict'
    
    spark-submit --master yarn --py-files dist/dlpredictor-2.0.0-py2.7.egg,dist/imscommon-2.0.0-py2.7.egg,dist/predictor_dl_model-1.0.0-py2.7.egg --num-executors 3 --executor-cores 3 --jars lib/elasticsearch-hadoop-6.8.0.jar dlpredictor/main_spark_es.py conf/config.yml '2020-02-08' 's32' '1' 'http://10.193.217.105:8501/v1/models/faezeh:predict'

fi