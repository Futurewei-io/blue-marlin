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

log = logging.getLogger()

SHUFFLE_BUFFER = 100


def __data_parser(serialized_example):

    features = tf.parse_single_example(serialized_example,
                                       features={'uph': tf.FixedLenFeature([], tf.string),
                                                 'price_cat_0_n': tf.FixedLenFeature([], tf.float32),
                                                 'price_cat_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'price_cat_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'price_cat_3_n': tf.FixedLenFeature([], tf.float32),
                                                 'hour': tf.FixedLenFeature([], tf.int64),
                                                 'g_g_m_n': tf.FixedLenFeature([], tf.float32),
                                                 'g_g_f_n': tf.FixedLenFeature([], tf.float32),
                                                 'g_g_x_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_3_n': tf.FixedLenFeature([], tf.float32),
                                                 'a_4_n': tf.FixedLenFeature([], tf.float32),
                                                 't_3G_n': tf.FixedLenFeature([], tf.float32),
                                                 't_4G_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_3_n': tf.FixedLenFeature([], tf.float32),
                                                 'dow_sin': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'dow_cos': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'ts': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True)
                                                 })

    uckey = tf.cast(features['uph'], tf.string)
    price_cat_0 = tf.cast(features['price_cat_0_n'], tf.float32)
    price_cat_1 = tf.cast(features['price_cat_1_n'], tf.float32)
    price_cat_2 = tf.cast(features['price_cat_2_n'], tf.float32)
    price_cat_3 = tf.cast(features['price_cat_3_n'], tf.float32)
    hour = tf.cast(features['hour'], tf.float32)
    gender_x = tf.cast(features['g_g_x_n'], tf.float32)
    gender_f = tf.cast(features['g_g_f_n'], tf.float32)
    gender_m = tf.cast(features['g_g_m_n'], tf.float32)
    age_1 = tf.cast(features['a_1_n'], tf.float32)
    age_2 = tf.cast(features['a_2_n'], tf.float32)
    age_3 = tf.cast(features['a_3_n'], tf.float32)
    age_4 = tf.cast(features['a_4_n'], tf.float32)
    t_3G = tf.cast(features['t_3G_n'], tf.float32)
    t_4G = tf.cast(features['t_4G_n'], tf.float32)
    si_1 = tf.cast(features['si_1_n'], tf.float32)
    si_2 = tf.cast(features['si_2_n'], tf.float32)
    si_3 = tf.cast(features['si_3_n'], tf.float32)
    hits = tf.cast(features['ts'], tf.float32)
    dow_sin = tf.cast(features['dow_sin'], tf.float32)
    dow_cos = tf.cast(features['dow_cos'], tf.float32)
    hits = tf.cast(features['ts'], tf.float32)

    return hour, gender_f, gender_m, gender_x, hits, price_cat_0, price_cat_1, price_cat_2, \
        price_cat_3, age_1, age_2, age_3, age_4, t_3G, \
        t_4G, si_1, si_2, si_3, uckey, dow_cos, dow_sin


def run(cfg):

    with open(cfg['tf_statistics_path'], 'rb') as f:
        tf_stat = pickle.load(f)

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

    holidays = numpy.ones((duration,), dtype=float)

    lagged_ix = numpy.ones((duration, 4), dtype=float)
    quarter_autocorr = numpy.ones((batch_size,), dtype=float)
    page_popularity = numpy.ones((batch_size,), dtype=float)

    date_list = tf_stat['days']

    dow =[(a,b) for a,b in zip(tf_stat['dow_sin'],tf_stat['dow_cos'])]
    
    with tf.Session() as sess:
        x = sess.run(next_el)
        l1 = x[19][0]
        l2 = x[20][0]
        m = [[v] for v in l1]
        [m[i].append(l2[i]) for i in range(0, len(l1))]
        page_indx = list(x[18])

        tensors = dict(
            hits=pd.DataFrame(x[4], index=page_indx, columns=date_list),
            lagged_ix=lagged_ix,
            page_ix=page_indx,
            pf_age=pd.DataFrame(x[9:13], columns=page_indx).T,
            pf_si=pd.DataFrame(x[15:18], columns=page_indx).T,
            pf_network=pd.DataFrame(x[13:15], columns=page_indx).T,
            pf_price_cat=pd.DataFrame(x[5:9], columns=page_indx).T,
            pf_gender=pd.DataFrame(x[1:4], columns=page_indx).T,
            page_popularity=page_popularity,
            quarter_autocorr=quarter_autocorr,
            dow=dow,
            holiday=holidays)
        
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

    # This is the format for the shell arguments
    # python trainer/tfrecord_reader.py --add_days=63 factdata-12192019.tfrecord trainer/data/vars

    # parser = argparse.ArgumentParser(description='Prepare data')
    # parser.add_argument('config_file')
    # parser.add_argument('data_dir')
    # parser.add_argument('--valid_threshold', default=0.0, type=float, help="Series minimal length threshold (pct of data length)")
    # parser.add_argument('--add_days', default=64, type=int, help="Add N days in a future for prediction")
    # parser.add_argument('--start', help="Effective start date. Data before the start is dropped")
    # parser.add_argument('--end', help="Effective end date. Data past the end is dropped")
    # parser.add_argument('--corr_backoffset', default=0, type=int, help='Offset for correlation calculation')
    # args = parser.parse_args()

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    cfg = cfg['tfrecorder_reader']

    run(cfg)
