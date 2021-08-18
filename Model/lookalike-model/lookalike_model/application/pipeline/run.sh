#!/bin/bash

# spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g seed_user_selector.py config.yml "29"

spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g score_generator.py config.yml

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict score_vector_table.py config.yml

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 8G --driver-memory 8G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict score_vector_rebucketing.py config.yml

spark-submit --master yarn --num-executors 20 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5g --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict top_n_similarity_table_generator.py config.yml

# optional
spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g validation.py config.yml "29"

