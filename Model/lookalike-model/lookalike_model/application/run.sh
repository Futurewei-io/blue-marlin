#!/bin/bash
spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g seed_user_selector.py config.yml ;
spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g score_generator.py config.yml ;
spark-submit --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --master yarn --conf spark.driver.maxResultSize=8g distance_table_list.py config.yml ;

