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
                                                 't_5G_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_1_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_2_n': tf.FixedLenFeature([], tf.float32),
                                                 'si_3_n': tf.FixedLenFeature([], tf.float32),
                                                 'dow_sin': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'dow_cos': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'ts_n': tf.FixedLenSequenceFeature([], tf.float32, allow_missing=True),
                                                 'page_popularity_n': tf.FixedLenFeature([], tf.float32),
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
    t_5G = tf.cast(features['t_5G_n'], tf.float32)
    si_1 = tf.cast(features['si_1_n'], tf.float32)
    si_2 = tf.cast(features['si_2_n'], tf.float32)
    si_3 = tf.cast(features['si_3_n'], tf.float32)
    #r = tf.cast(features['r'], tf.float32)
    hits = tf.cast(features['ts_n'], tf.float32)
    page_popularity =  tf.cast(features['page_popularity_n'], tf.float32)

    return hour, gender_f, gender_m, gender_x, hits, price_cat_0, price_cat_1, price_cat_2, \
        price_cat_3, age_1, age_2, age_3, age_4, t_3G, \
        t_4G, t_5G, si_1, si_2, si_3, uckey, page_popularity

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
        return pd.Series(data=base_index.loc[dates].fillna(-1).astype(np.int16).values, index=date_range)

    return [lag(pd.DateOffset(months=m)) for m in (1, 2)]

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

    # lagged_ix = numpy.ones((duration, 4), dtype=float)
    # lagged_ix = np.where(lagged_ix == 1, -1, lagged_ix)
    lagged_ix = np.stack(lag_indexes(tf_stat), axis=-1)
    quarter_autocorr = numpy.ones((batch_size,), dtype=float)

    date_list = tf_stat['days']
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
        page_indx = list(x[19])

        tensors = dict(
            hits=pd.DataFrame(x[4], index=page_indx, columns=date_list),
            lagged_ix=lagged_ix,
            page_ix=page_indx,
            pf_age=pd.DataFrame(x[9:13], columns=page_indx, index = (1,2,3,4)).T,
            pf_si=pd.DataFrame(x[16:19], columns=page_indx, index=(1,2,3)).T,
            pf_network=pd.DataFrame(x[13:16], columns=page_indx, index=('3G','4G','5G')).T,
            pf_price_cat=pd.DataFrame(x[5:9], columns=page_indx, index=('pc0','pc1','pc2','pc3')).T,
            pf_gender=pd.DataFrame(x[1:4], columns=page_indx, index= ('f','m','x')).T,
            page_popularity=x[20],
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

    holidays = cfg['pipeline']['holidays']
    cfg = cfg['tfrecorder_reader']
    cfg['holidays'] = holidays

    run(cfg)
