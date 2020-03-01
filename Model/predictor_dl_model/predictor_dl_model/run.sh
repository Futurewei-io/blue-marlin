#!/bin/bash

#Preparing data and save the results as <config.pipeline.tmp_table_name> 
if false
then
    spark-submit pipeline/main.py config.yml [1]
fi

#Saving tables as <config.pipeline.tfrecords_path>
if false
then
    spark-submit --jars spark-tensorflow-connector_2.11-1.15.0.jar pipeline/main.py config.yml [2]
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
if true
then
    python trainer/tfrecord_reader.py config.yml
    python trainer/trainer.py config.yml 
fi

# Saving the model
if true
then
    python trainer/save_model.py --data_dir=data/vars --ckpt_dir=data/cpt/s32 --saved_dir=data/vars --model_version=1
fi

# Saving the model in elasticsearch
if false
then
    python pipeline/pickle_to_es.py config.yml
fi