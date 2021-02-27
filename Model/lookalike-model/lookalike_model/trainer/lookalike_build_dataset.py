#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
 
#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

'''
The script reads the data from tfrecord and generates the dataset for the trainer.
'''
import numpy as np
import os
import pandas as pd
import random
import pickle
import tensorflow as tf
import ast
import argparse
import yaml
import json



random.seed(1234)

def __data_parser(serialized_example):

    features = tf.parse_single_example(serialized_example,
                                       features={'interval_starting_time': tf.FixedLenSequenceFeature([], tf.int64, allow_missing=True),
                                                 'keywords' :tf.FixedLenSequenceFeature([], tf.string, allow_missing=True),
                                                 'did_index': tf.FixedLenFeature([], tf.int64),
                                                 'click_counts': tf.FixedLenSequenceFeature([], tf.string, allow_missing=True),
                                                 'show_counts': tf.FixedLenSequenceFeature([], tf.string, allow_missing=True),
                                                 # 'media_category_index': tf.FixedLenFeature([], tf.int64),
                                                 # 'net_type_index': tf.FixedLenFeature([], tf.int64),
                                                 'gender': tf.FixedLenFeature([], tf.int64),
                                                 'age': tf.FixedLenFeature([], tf.int64),
                                                 'did': tf.FixedLenFeature([], tf.string),


                                                 })
    did_str = tf.cast(features['did'], tf.string)
    time_interval = tf.cast(features['interval_starting_time'], tf.int64)
    keyword = tf.cast(features['keywords'], tf.string)
    ucdoc = tf.cast(features['did_index'], tf.int64)
    click_counts = tf.cast(features['click_counts'], tf.string)
    show_counts = tf.cast(features['show_counts'], tf.string)
    # media_category = tf.cast(features['media_category_index'], tf.int64)
    # net_type_index = tf.cast(features['net_type_index'], tf.int64)
    gender = tf.cast(features['gender'], tf.int64)
    age = tf.cast(features['age'], tf.int64)

    return time_interval, ucdoc, click_counts, show_counts, gender, age, keyword, did_str




def read_files(csv_location, columns_name):
    names = []

    for file in os.listdir(csv_location):
        if file.startswith("part"):
            names.append(file)
    file_paths = [os.path.join(csv_location, name) for name in names]
    li = []
    for filename in file_paths:
        df = pd.read_csv(filename, names=columns_name )
        li.append(df)
    frame = pd.concat(li, ignore_index=True)
    return frame

def str_to_intlist(table):
    ji = []
    for k in [table[j].decode().split(",") for j in range(len(table))]:
        s = []
        for a in k:
            b = int(a.split(":")[1])
            s.append(b)
        ji.append(s)
    return ji

def flatten(lst):
    f = [y for x in lst for y in x]
    return f


train_set = []
test_set = []
pos_sample = []
user_att = []

def run(cfg):
    date = cfg['pipeline']['cutting_date']
    sts = cfg['pipeline']['tfrecords']
    tfrecord_location = sts['tfrecords_hdfs_path']
    names = []
    for file in os.listdir(tfrecord_location):
        if file.startswith("part"):
            names.append(file)
    file_paths = [os.path.join(tfrecord_location, name) for name in names]

    # read and make the dataset from tfrecord
    dataset = tf.data.TFRecordDataset(file_paths)
    dataset = dataset.map(__data_parser)

    iterator = dataset.make_one_shot_iterator()
    next_el = iterator.get_next()

    #time_interval, ucdoc, click_counts, show_counts, adv_type_index, net_type_index, gender, age
    sess=tf.Session()
    length = cfg['pipeline']['length']
    keyword_set = set()
    ucdoc_lst = []
    label = []

    stats = sts['tfrecords_statistics_path']
    with open(stats, 'rb') as f:
        ucdoc_num = pickle.load(f)['distinct_records_count']
    counter = 0
    for i in range(ucdoc_num):
        x = sess.run(next_el)
        log = list(x[0:7])
        time_interval, ucdoc, click_counts, show_counts, gender, age, keyword = log[0], log[1], log[2], log[3], log[4], log[5], log[6]

        keyword_int = [[int(i) for i in keyword[j].decode().split(",")] for j in range(len(keyword))]
        show_counts_list = str_to_intlist(show_counts)
        click_counts_list = str_to_intlist(click_counts)
        keyword_set.update({y for x in keyword_int for y in x})

        if np.count_nonzero([item for sublist in click_counts_list for item in sublist]) != 0:
            counter+=1
            indicing = len([x for x in time_interval if x >= date])

            ## training set
            keyword_int_train = keyword_int[indicing:]
            click_counts_train = click_counts_list[indicing:]

            ## training dataset
            ucdoc_lst.append(ucdoc)
            for m in range(len(click_counts_train)):
                for n in range(len(click_counts_train[m])):
                    if (click_counts_train[m][n] != 0):
                        pos = (ucdoc, flatten(keyword_int_train[m+1:m+1+length]), keyword_int_train[m][n], 1)
                        pos_sample.append(1)
                        if len(pos[1])>=1:
                            train_set.append(pos)
                    elif (m%5==0 and n%2==0):
                        neg = (ucdoc, flatten(keyword_int_train[m+1:m+1+length]), keyword_int_train[m][n], 0)
                        if len(neg[1]) >= 1:
                            train_set.append(neg)

            ## testing set
            keyword_int_test = keyword_int[:indicing]
            click_counts_test = click_counts_list[:indicing]

            for m in range(len(click_counts_test)):
                for n in range(len(click_counts_test[m])):
                    if (click_counts_test[m][n] != 0):
                        pos = (ucdoc, flatten(keyword_int_test[m+1:m+1+length]), (keyword_int_test[m][n],keyword_int_test[m][n]))
                        pos_sample.append(1)
                        if len(pos[1])>=1:
                            test_set.append(pos)
                            label.append(1)
                    elif (m%5==0 and n%2==0):
                        neg = (ucdoc, flatten(keyword_int_test[m+1:m+1+length]), (keyword_int_test[m][n],keyword_int_test[m][n]) )
                        if len(neg[1]) >= 1:
                            test_set.append(neg)
                            label.append(0)



            dense_att = [ucdoc, gender, age]
            user_att.append(dense_att)

        ##testing dataset

    cate_list = np.array([x for x in range(30)])
    user_count, item_count , cate_count = len(set(ucdoc_lst)) , 30, 30
    print(counter)
    with open('label_gdin_30.pkl', 'wb') as f:
        pickle.dump(label, f, pickle.HIGHEST_PROTOCOL)
        pickle.dump(user_att,f, pickle.HIGHEST_PROTOCOL )

    with open('ad_dataset_gdin_30.pkl', 'wb') as f:
        pickle.dump(train_set, f, pickle.HIGHEST_PROTOCOL)
        pickle.dump(test_set, f, pickle.HIGHEST_PROTOCOL)
        pickle.dump(cate_list, f, pickle.HIGHEST_PROTOCOL)
        pickle.dump((user_count, item_count, cate_count), f, pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    run(cfg)

