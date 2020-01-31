#!/bin/bash

#Start the predictor
if true
then
    spark-submit --jars lib/elasticsearch-hadoop-6.5.2.jar dlpredictor/main_spark_es.py conf/config.yml 's32' '1' 'http://10.193.217.108:8501/v1/models/faezeh:predict'
fi
