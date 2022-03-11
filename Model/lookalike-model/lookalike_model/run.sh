#!/bin/bash

# main_keywords: identify the keywords with proportion of traffic above set threshold.
if false
then
    # generate the effective keywords table. 
    spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_keywords.py config.yml
fi

# main_clean: preparing cleaned persona, click and show logs data.
if false
then
    # generate three tables: new persona, new clicklog, new showlog. 
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_clean.py config.yml
fi

# main_logs: generating union logs and cleaning the logs.
if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_logs.py config.yml
fi

# main_trainready: generating the trainready data by grouping the data.
if false
then
    spark-submit --master yarn --executor-memory 16G --driver-memory 24G --num-executors 10 --executor-cores 5 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_trainready.py config.yml
fi

#Saving tables as <config.pipeline.tfrecords_path>
if false
then
    # generate tf records: din tf record in hdfs.
    # after the tf records folder is generated in hdfs, use 'hadoop fs -copyToLocal' to copy it to local.
    spark-submit --master yarn --executor-memory 16G --driver-memory 24G --num-executors 10 --executor-cores 5 --jars spark-tensorflow-connector_2.11-1.15.0.jar pipeline/main_tfrecord_generator.py config.yml
fi
