#!/bin/bash

if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/show_config.py config.yml
fi

#This module transform T1 : request based factdata to T2 : compatible factdata for rest of pipelie
if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --conf spark.driver.maxResultSize=5G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_rti_transform.py config.yml
fi

#Preparing the data by filtering reliable si, remapping r, ipl and recalculating bucket-ids
#This part might be optional if uckeys have stable slot-id with region data
if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_filter_si_region_bucket.py config.yml
fi

#Preparing ts data and save the results as <config.pipeline.time_series.ts_tmp_table_name> 
if false
then  
    spark-submit --master yarn --py-files pipeline/transform.py --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_ts.py config.yml
fi

#Run outlier filter and save the results as <config.pipeline.time_series.{product_tag}_{pipeline_tag}_tmp_outlier> 
if false
then 
    spark-submit --master yarn --py-files pipeline/transform.py --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_outlier.py config.yml
fi

#Preparing clustering
if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_cluster.py config.yml
fi

#generating distribution
if false
then
    spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_distribution.py config.yml
fi

#generating clusters analysis
#if false
#then
    # Analysis step NOT PART OF PIPELIE
    # spark-submit --master yarn --num-executors 10 --executor-cores 5 pipeline/main_clusters_analysis.py config.yml 
#fi

#Preparing normalization
if false
then
    spark-submit --master yarn --py-files pipeline/transform.py --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_norm.py config.yml
fi

#Saving tables as <config.pipeline.tfrecords_path>
#For spark3 obtain jar file from https://github.com/linkedin/spark-tfrecord
if false
then
    spark-submit --jars spark-tensorflow-connector_2.11-1.15.0.jar pipeline/main_tfrecords.py config.yml
fi

#Saving tfrecords from hdfs to local drive
if false
then
    tfrecords_hdfs_path=$(python pipeline/get_config_attr.py config.yml pipeline tfrecords_hdfs_path >&1)
    tfrecords_local_path=$(python pipeline/get_config_attr.py config.yml tfrecorder_reader tfrecords_local_path >&1)
    echo $tfrecords_hdfs_path '--->' $tfrecords_local_path
    rm -r $tfrecords_local_path
    hdfs dfs -get $tfrecords_hdfs_path $tfrecords_local_path
fi

#Training the model
if false
then
    python trainer/tfrecord_reader.py config.yml
    python trainer/trainer.py config.yml 
fi

# Saving the model
if false
then
    python trainer/save_model.py --data_dir=data/vars --ckpt_dir=data/cpt/s32 --saved_dir=data/vars --model_version=1
fi

# Saving the model in elasticsearch
if false
then
    python pipeline/pickle_to_es.py config.yml
fi