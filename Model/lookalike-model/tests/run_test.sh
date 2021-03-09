#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# test_main_clean: preparing cleaned persona, click and show logs data.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_clean.py
fi

# test_main_logs: merges click and show log data.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_logs.py
fi

# test_main_logs: merges click and show log data.
if true
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_trainready.py
fi
