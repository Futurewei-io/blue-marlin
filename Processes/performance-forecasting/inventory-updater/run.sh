#!/bin/bash

if true
then
    spark-submit --master yarn --num-executors 15 --executor-cores 15 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=8G --jars lib/elasticsearch-hadoop-7.6.2.jar inventory_updater/inventory_updater.py conf/config.yml
    spark-submit --num-executors 10 --executor-cores 5 tests/test_inventory_updater.py
fi