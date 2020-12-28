#!/bin/bash

if true
then
    spark-submit --master yarn --num-executors 15 --executor-cores 15 --executor-memory 16G --driver-memory 16G --py-files lib/din_model-1.0.0-py2.7.egg --conf spark.driver.maxResultSize=16G  ctr_score_generator/ctr_score_generator.py conf/config.yml
    spark-submit --num-executors 10 --executor-cores 5 tests/test_ctr_score_generator.py
fi