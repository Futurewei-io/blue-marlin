#!/bin/bash

# main_clean: preparing cleaned persona, click and show logs data.
if false
then
    # generate three tables: new persona, new clicklog, new showlog. 
    spark-submit --master yarn --driver-memory 32G --num-executors 16 --executor-cores 5 pipeline/main_clean.py config.yml
fi

# main_logs: generating union logs and cleaning the logs.
if false
then
    spark-submit --master yarn --driver-memory 32G --num-executors 16 --executor-cores 5 pipeline/main_logs.py config.yml 
fi

# Optional step: main_logs_with_regions: inject region ids to the logs table which don't have any geo or region information.
if false
then
    spark-submit --master yarn --driver-memory 32G --num-executors 16 --executor-cores 5 pipeline/main_logs_with_regions.py config.yml 
fi

# main_trainready: generating the trainready data by grouping the data.
if false
then
    spark-submit --master yarn --driver-memory 32G --num-executors 16 --executor-cores 5 pipeline/main_trainready.py config.yml 
fi

#Saving tables as <config.pipeline.tfrecords_path>
if false
then
    # generate tf records: din tf record in hdfs.
    # after the tf records folder is generated in hdfs, use 'hadoop fs -copyToLocal' to copy it to local.
    spark-submit --jars spark-tensorflow-connector_2.11-1.15.0.jar pipeline/main_tfrecords.py config.yml 
fi
