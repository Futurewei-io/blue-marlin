# Copyright 2019, Futurewei Technologies
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import tensorflow as tf
from feeder import VarFeeder
import os
import argparse
import pandas as pd
import numpy
import logging
import yaml
import datetime
import math
import numpy as np
from typing import Tuple
import pickle
from predictor_dl_model.pipeline.util import get_dow
from typing import Tuple, Dict, Collection, List
from pyhive import hive
import json

log = logging.getLogger()

SHUFFLE_BUFFER = 100

def fill_isolated_zeros(x_in):
    # x_out = x_in
    for i in range(x_in.shape[0]):
        ind = np.where(x_in[i,:]!=0)
        s, e = ind[0][0], ind[0][-1]
        ind_zero = np.where(x_in[i,s:e]==0)
        if ind_zero:
            for j in ind_zero[0]:
                ind_here = s + j
                prev = max(s, ind_here-1)
                post = min(e, ind_here+1)
                if x_in[i,prev]!=0 and x_in[i,post]!=0:
                    x_in[i, ind_here] = (x_in[i,prev] + x_in[i,post]) / 2
                    # x_out[i, ind_here] = (x_in[i,prev] + x_in[i,post]) / 2
    return

def __data_parser(serialized_example):

    features = tf.parse_single_example(serialized_example,
                                       features={'page_ix': tf.FixedLenFeature([], tf.string),
                                                 'price_cat_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'price_cat_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'price_cat_3_n': tf.FixedLenFeature([], tf.float32),
                                                 # 'hour': tf.FixedLenFeature([], tf.int64),
                                                 'g_g_m_n': tf.FixedLenFeature([], tf.float32),
                                                 'g_g_f_n': tf.FixedLenFeature([], tf.float32),
                                                 'g_g_x_n': tf.FixedLenFeature([], tf.float32),
                                                 'g__n': tf.FixedLenFeature([], tf.float32),
                                                 'a__n': tf.FixedLenFeature([], tf.float32),
                                                 'a_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_3_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_4_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_5_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_6_n': tf.FixedLenFeature([], tf.float32),
                                                 't_3G_n': tf.FixedLenFeature([], tf.float32),
                                                 't_4G_n': tf.FixedLenFeature([], tf.float32),
                                                 't_UNKNOWN_n': tf.FixedLenFeature([], tf.float32),
                                                 't_WIFI_n': tf.FixedLenFeature([], tf.float32),
                                                 't_2G_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_vec_n': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'ts_n': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'p_n': tf.FixedLenFeature([], tf.float32),
                                                 })

    uckey = tf.cast(features['page_ix'], tf.string)
    price_cat_1 = tf.cast(features['price_cat_1_n'], tf.float32)
    price_cat_2 = tf.cast(features['price_cat_2_n'], tf.float32)
    price_cat_3 = tf.cast(features['price_cat_3_n'], tf.float32)
    gender_x = tf.cast(features['g_g_x_n'], tf.float32)
    gender_f = tf.cast(features['g_g_f_n'], tf.float32)
    gender_m = tf.cast(features['g_g_m_n'], tf.float32)
    gender_no = tf.cast(features['g__n'], tf.float32)
    age_no = tf.cast(features['a__n'], tf.float32)
    age_1 = tf.cast(features['a_1_n'], tf.float32)
    age_2 = tf.cast(features['a_2_n'], tf.float32)
    age_3 = tf.cast(features['a_3_n'], tf.float32)
    age_4 = tf.cast(features['a_4_n'], tf.float32)
    age_5 = tf.cast(features['a_5_n'], tf.float32)
    age_6 = tf.cast(features['a_6_n'], tf.float32)
    t_3G = tf.cast(features['t_3G_n'], tf.float32)
    t_4G = tf.cast(features['t_4G_n'], tf.float32)
    t_2G =tf.cast(features['t_2G_n'], tf.float32)
    t_UNKNOWN =tf.cast(features['t_UNKNOWN_n'], tf.float32)
    t_WIFI =tf.cast(features['t_WIFI_n'], tf.float32)
    si = tf.cast(features['si_vec_n'], tf.float32)
    hits = tf.cast(features['ts_n'], tf.float32)
    page_popularity =  tf.cast(features['p_n'], tf.float32)


    return uckey, price_cat_1, price_cat_2,price_cat_3, gender_no, gender_f, gender_m, gender_x, age_no,age_1, age_2, age_3, age_4,age_5, age_6, \
           t_2G, t_3G,t_4G,t_UNKNOWN, t_WIFI, si,hits,page_popularity

def holiday_norm(day):
    return math.sin(day), math.cos(day)

def lag_indexes(tf_stat)-> List[pd.Series]:
    """
    Calculates indexes for 3, 6, 9, 12 months backward lag for the given date range
    :param begin: start of date range
    :param end: end of date range
    :return: List of 4 Series, one for each lag. For each Series, index is date in range(begin, end), value is an index
     of target (lagged) date in a same Series. If target date is out of (begin,end) range, index is -1
    """
    date_range = pd.date_range(tf_stat['days'][0],tf_stat['days'][-1])
    # key is date, value is day index
    base_index = pd.Series(np.arange(0, len(date_range)),index=date_range)

    def lag(offset):
        dates = date_range - offset
        return pd.Series(data=base_index[dates].fillna(-1).astype(np.int16).values, index=date_range)

    return [lag(pd.DateOffset(months=m)) for m in (1, 2)]

def run(cfg):
    conn = hive.Connection(host='10.213.37.46', username='hive', password='hive', auth='CUSTOM')
    cursor = conn.cursor()
    cursor.execute('select * from dlpm_11092020_model_stat')
    stat_model = cursor.fetchone()
    model_info= json.loads(stat_model[0])
    stat_info = json.loads(stat_model[1])


    names = []
    tfrecord_location = cfg['tfrecords_local_path']
    for file in os.listdir(tfrecord_location):
        if file.startswith("part"):
            names.append(file)    
    file_paths = [os.path.join(tfrecord_location, name) for name in names]

    # read and make the dataset from tfrecord
    dataset = tf.data.TFRecordDataset(file_paths)
    dataset = dataset.map(__data_parser)
    
    batch_size = cfg['batch_size']
    duration = cfg['duration']

    dataset = dataset.batch(batch_size).shuffle(SHUFFLE_BUFFER)
    iterator = dataset.make_one_shot_iterator()
    next_el = iterator.get_next()

    # lagged_ix = numpy.ones((duration, 4), dtype=float)
    # lagged_ix = np.where(lagged_ix == 1, -1, lagged_ix)
    lagged_ix = np.stack(lag_indexes(model_info), axis=-1)
    # quarter_autocorr = numpy.ones((batch_size,), dtype=float)

    date_list = model_info['days']
    dow =get_dow(date_list)

    holiday_list = cfg['holidays']

    holidays = [1 if _ in holiday_list else 0 for _ in date_list]
    a_list = []
    b_list = []
    for _ in holidays:
        a,b =holiday_norm(_)
        a_list.append(a)
        b_list.append(b)
    holiday = (a_list, b_list)

    with tf.Session() as sess:

        x = sess.run(next_el)
        quarter_autocorr = numpy.ones((x[0].size,), dtype=float)
        page_indx = list(x[0])

        fill_isolated_zeros(x[21])
        tensors = dict(
            hits=pd.DataFrame(x[21], index=page_indx, columns=date_list),
            lagged_ix=lagged_ix,
            page_ix=page_indx,
            pf_age=pd.DataFrame(x[8:15], columns=page_indx, index = (1,2,3,4,5,6,7)).T,
            pf_si=pd.DataFrame(x[20], index = page_indx ),
            pf_network=pd.DataFrame(x[15:20], columns=page_indx, index=('2G','3G','4G', 'UNKNOWN','WIFI')).T,
            pf_price_cat=pd.DataFrame(x[1:4], columns=page_indx, index=('pc1','pc2','pc3')).T,
            pf_gender=pd.DataFrame(x[4:8], columns=page_indx, index= ('none','f','m','x')).T,
            page_popularity=x[22],
            # page_popularity = quarter_autocorr,
            quarter_autocorr= quarter_autocorr,
            dow=pd.DataFrame(dow).T,
            holiday=pd.DataFrame(holiday).T)

        data_len = tensors['hits'].shape[1]
        plain = dict(
            data_days=data_len - cfg['add_days'],
            features_days=data_len,
            data_start=date_list[0],
            data_end=date_list[-1],
            features_end=date_list[-1],
            n_pages=batch_size
        )
        VarFeeder(cfg['data_dir'], tensors, plain)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    holidays = cfg['pipeline']['normalization']['holidays']
    cfg = cfg['tfrecorder_reader']
    cfg['holidays'] = holidays

    run(cfg)
