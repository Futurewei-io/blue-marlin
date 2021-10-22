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

"""
A client that talks to tensorflow_serving loaded with kaggle model.

The client read kaggle feature data set, queries the service with
such feature data to get predictions, and calculates the inference error rate.

"""

import pickle
import json
import requests
import numpy as np
from predictor_dl_model.pipeline.util import get_dow
from typing import List
import pandas as pd

FORWARD_OFFSET = 11


def get_start_end(records_len, train_window):
    start = records_len - train_window - FORWARD_OFFSET
    end = records_len - FORWARD_OFFSET
    return start, end


def lag_indexes(day_list):
    """
    Calculates indexes for 3, 6, 9, 12 months backward lag for the given date range
    :param begin: start of date range
    :param end: end of date range
    :return: List of 4 Series, one for each lag. For each Series, index is date in range(begin, end), value is an index
     of target (lagged) date in a same Series. If target date is out of (begin,end) range, index is -1
    """
    date_range = pd.date_range(day_list[0], day_list[-1])
    # key is date, value is day index
    base_index = pd.Series(np.arange(0, len(date_range)), index=date_range)

    def lag(offset):
        dates = date_range - offset
        return pd.Series(data=base_index[dates].fillna(-1).astype(np.int64).values, index=date_range)

    return [lag(pd.DateOffset(months=m)) for m in (1, 2)]


# def cut(hits, start, end):
#     """
#     Cuts [start:end] diapason from input data
#     :param hits: hits timeseries
#     :param start: start index
#     :param end: end index
#     :return: tuple (train_hits, test_hits, dow, lagged_hits)
#     """
#     # Pad hits to ensure we have enough array length for prediction
#
#     hits = np.concatenate([hits, np.fill([10], np.NaN)], axis=0)
#     cropped_hit = hits[start:end]
#
#     # Cut lagged hits
#     # gather() accepts only int32 indexes
#     cropped_lags = np.cast(lagged_ix[start:end], np.int32)
#     # Mask for -1 (no data) lag indexes
#     lag_mask = cropped_lags < 0
#     # Convert -1 to 0 for gather(), it don't accept anything exotic
#     cropped_lags = np.maximum(cropped_lags, 0)
#     # Translate lag indexes to hit values
#     lagged_hit = np.gather(hits, cropped_lags)
#     # Convert masked (see above) or NaN lagged hits to zeros
#     lag_zeros = np.zeros_like(lagged_hit)
#     lagged_hit = np.where(lag_mask | np.is_nan(lagged_hit), lag_zeros, lagged_hit)
#
#     # Split for train and test
#     x_hits, y_hits = np.split(cropped_hit, [60, 10], axis=0)


def make_pred_input(duration, train_window, predict_window, full_record_exp, x_hits, dow, lagged_ix, pf_age, pf_si, pf_network,
                    pf_gender, page_ix, pf_price_cat,
                    page_popularity, quarter_autocorr):
    """
    Main method. Assembles input data into final tensors
    """
    # Split day of week to train and test

    # x_dow, y_dow = tf.split(dow, [train_window, predict_window], axis=0)
    x_dow = dow[:train_window]
    y_dow = dow[train_window:]
    # Normalize hits
    mean = np.mean(x_hits)
    std = np.std(x_hits)
    if std == 0:
        std = 1
    norm_x_hits = [(_ - mean) / std for _ in x_hits]

    # lagged_ix = np.where(lagged_ix==-1, np.NaN, lagged_ix)
    cropped_lags = lagged_ix
    # Mask for -1 (no data) lag indexes
    lag_mask = cropped_lags < 0
    # Convert -1 to 0 for gather(), it don't accept anything exotic
    cropped_lags = np.maximum(cropped_lags, 0)
    # Translate lag indexes to hit values
    lagged_hit = np.take(full_record_exp, cropped_lags)
    # Convert masked (see above) or NaN lagged hits to zeros
    lag_zeros = np.zeros_like(lagged_hit)
    lagged_hit = np.where(lag_mask | np.isnan(
        lagged_hit), lag_zeros, lagged_hit)
    start, end = get_start_end(duration, train_window)
    lagged_hit = lagged_hit[start:end+predict_window]
    norm_lagged_hits = np.divide(np.subtract(lagged_hit, mean), std)

    #stat = {'mean':mean, 'std':std}
    # Split lagged hits to train and predict
    x_lagged, y_lagged = norm_lagged_hits[:
                                          train_window], norm_lagged_hits[train_window:]

    # Combine all page features into single tensor
    stacked_features = np.stack([page_popularity, quarter_autocorr])
    flat_ucdoc_features = np.concatenate([pf_age, pf_si, pf_network, pf_gender, pf_price_cat, stacked_features],
                                         axis=0)  # pf_region
    ucdoc_features = np.expand_dims(flat_ucdoc_features, 0)

    # Train features
    x_features = np.concatenate([
        # [n_days] -> [n_days, 1]
        np.expand_dims(norm_x_hits, -1),
        x_dow,
        x_lagged,
        # Stretch ucdoc_features to all training days
        # [1, features] -> [n_days, features]
        np.tile(ucdoc_features, [train_window, 1])], axis=1
    )
    y_features = np.concatenate([
        # [n_days] -> [n_days, 1]
        y_dow,
        y_lagged,
        # Stretch ucdoc_features to all testing days
        # [1, features] -> [n_days, features]
        np.tile(ucdoc_features, [predict_window, 1])
    ], axis=1)

    return x_hits, x_features, norm_x_hits, x_lagged, y_features, mean, std, flat_ucdoc_features, page_ix  # , stat


def get_predict_post_body(model_stats, day_list, day_list_cut, uckey, age, si, network, gender, media, ip_location, full_record,  hits, hour, price_cat):

    price_cat = str(price_cat)
    hour = str(hour)

    train_window = model_stats['model']['train_window']  # comes from cfg
    predict_window = model_stats['model']['predict_window']  # comes from cfg
    x_hits = np.log(np.add(hits, 1)).tolist()  # ln + 1
    full_record_exp = np.log(np.add(full_record, 1)).tolist()

    if len(day_list_cut) != train_window+predict_window:
        raise Exception('day_list_cut and train window + predicti_window do not match. {} {} {}'.format(
            len(day_list_cut), train_window, predict_window))

    dow = get_dow(day_list_cut)
    dow = [[dow[0][i], dow[1][i]] for i in range(train_window+predict_window)]

    lagged_indx = np.stack(lag_indexes(day_list), axis=-1)
    # lagged_hits = [0 for i in range(2)]
    # lagged_hits = [lagged_hits for _ in range(train_window+predict_window)]

    m = model_stats['stats']
    pf_age = [(int(age == '1') - m['a_1'][0])/m['a_1'][1],
              (int(age == '2') - m['a_2'][0])/m['a_2'][1],
              (int(age == '3') - m['a_3'][0])/m['a_3'][1],
              (int(age == '4') - m['a_4'][0])/m['a_4'][1]]

    pf_si = [(int(si == '1') - m['si_1'][0])/m['si_1'][1],
             (int(si == '2') - m['si_2'][0])/m['si_2'][1],
             (int(si == '3') - m['si_3'][0])/m['si_3'][1]]

    pf_network = [
        (int(network == '3G') - m['t_3G'][0])/m['t_3G'][1],
        (int(network == '4G') - m['t_4G'][0])/m['t_4G'][1],
        (int(network == '5G') - m['t_5G'][0]) / m['t_5G'][1]]

    pf_gender = [(int(gender == 'g_f') - m['g_g_f'][0])/m['g_g_f'][1],
                 (int(gender == 'g_m') - m['g_g_m'][0])/m['g_g_m'][1],
                 (int(gender == 'g_x') - m['g_g_x'][0])/m['g_g_x'][1]]

    pf_price_cat = [(int(price_cat == '0') - m['price_cat_0'][0])/m['price_cat_0'][1],
                    (int(price_cat == '1') -
                     m['price_cat_1'][0])/m['price_cat_1'][1],
                    (int(price_cat == '2') -
                     m['price_cat_2'][0])/m['price_cat_2'][1],
                    (int(price_cat == '3') - m['price_cat_3'][0])/m['price_cat_3'][1]]

    page_ix = ','.join([uckey, price_cat, hour])

    # not used
    page_popularity = np.median(full_record_exp)
    page_popularity = (
        page_popularity - model_stats['stats']['page_popularity'][0]) / model_stats['stats']['page_popularity'][1]
    quarter_autocorr = 1

    duration = model_stats['model']['duration']
    # x_hits, x_features, norm_x_hits, x_lagged, y_features, mean, std, flat_ucdoc_features, page_ix
    truex, timex, normx, laggedx, timey, normmean, normstd, pgfeatures, pageix = make_pred_input(duration,
                                                                                                 train_window, predict_window, full_record_exp, x_hits, dow, lagged_indx, pf_age, pf_si, pf_network, pf_gender, page_ix,
                                                                                                 pf_price_cat, page_popularity, quarter_autocorr)

    # ys are not important]
    truey = [1 for _ in range(predict_window)]
    normy = [1 for _ in range(predict_window)]

    instance = {"truex": truex, "timex": timex.tolist(), "normx": normx,
                "laggedx": laggedx.tolist(),
                "truey": truey, "timey": timey.tolist(), "normy": normy,
                "normmean": normmean,
                "normstd": normstd, "page_features": pgfeatures.tolist(),
                "pageix": pageix}
    # print(instance)
    return instance  # , stat


def predict(serving_url, model_stats, day_list, uckey, age, si, network, gender, media, ip_location, records_hour_price_list):

    prediction_results = []
    for full_record, hour, price_cat in records_hour_price_list:
        train_window = model_stats['model']['train_window']
        prediction_window = model_stats['model']['predict_window']
        start = len(full_record)-train_window-FORWARD_OFFSET
        end = len(full_record)-FORWARD_OFFSET
        hits = full_record[start:end]
        day_list_cut = day_list[start:end+prediction_window]
        predict_day_list = day_list[end: end+prediction_window]

        uph = ','.join([uckey, str(price_cat), str(hour)])

        body = {"instances": []}
        instance = get_predict_post_body(
            model_stats, day_list, day_list_cut, uph, age, si, network, gender, media, ip_location, full_record, hits, hour, price_cat)
        body['instances'].append(instance)

        # URL="http://10.193.217.105:8501/v1/models/faezeh1:predict"
        body_json = json.dumps(body)
        result = requests.post(serving_url, data=body_json).json()
        predictions = result['predictions'][0]
        predictions = np.round(np.expm1(predictions))
        prediction_results.append(predictions.tolist())

    return prediction_results, predict_day_list


if __name__ == '__main__':  # record is equal to window size
    URL = "http://10.193.217.108:8501/v1/models/faezeh1:predict"

    model_stats = {
        "model": {
            "name": "s32",
            "version": 1,
            "duration": 90,
            "train_window": 60,
            "predict_window": 10
        },
        "stats": {
            "g_g_m": [
                0.32095959595959594,
                0.4668649491714752
            ],
            "g_g_f": [
                0.3654040404040404,
                0.4815635452904544
            ],
            "g_g_x": [
                0.31363636363636366,
                0.46398999646418304
            ],
            "a_1": [
                0.198989898989899,
                0.3992572317838901
            ],
            "a_2": [
                0.2474747474747475,
                0.4315630593164027
            ],
            "a_3": [
                0.295959595959596,
                0.45649211860504146
            ],
            "a_4": [
                0.25757575757575757,
                0.43731748751040456
            ],
            "t_3G": [
                0.3565656565656566,
                0.4790051176675845
            ],
            "t_4G": [
                0.29772727272727273,
                0.45727819223458205
            ],
            "t_5G": [
                0.3457070707070707,
                0.4756182644159981
            ],
            "si_1": [
                0.37424242424242427,
                0.4839470491115894
            ],
            "si_2": [
                0.4042929292929293,
                0.49077533664980666
            ],
            "si_3": [
                0.22146464646464648,
                0.4152500106648333
            ],
            "price_cat_0": [
                0.0,
                1.0
            ],
            "price_cat_1": [
                0.3333333333333333,
                0.4714243623012701
            ],
            "price_cat_2": [
                0.3333333333333333,
                0.47142436230126994
            ],
            "price_cat_3": [
                0.3333333333333333,
                0.47142436230126994
            ],
            "holiday_stats": [
                0.044444444444444446,
                0.20723493215097805
            ],
            "page_popularity": [3.9093487, 0.7969047]
        }
    }
    days = ['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04', '2018-01-05', '2018-01-06', '2018-01-07',
            '2018-01-08', '2018-01-09', '2018-01-10', '2018-01-11', '2018-01-12', '2018-01-13', '2018-01-14',
            '2018-01-15', '2018-01-16', '2018-01-17', '2018-01-18', '2018-01-19', '2018-01-20', '2018-01-21', '2018-01-22',
            '2018-01-23', '2018-01-24', '2018-01-25', '2018-01-26', '2018-01-27', '2018-01-28', '2018-01-29',
            '2018-01-30', '2018-01-31', '2018-02-01', '2018-02-02', '2018-02-03', '2018-02-04', '2018-02-05',
            '2018-02-06', '2018-02-07', '2018-02-08', '2018-02-09', '2018-02-10', '2018-02-11', '2018-02-12',
            '2018-02-13', '2018-02-14', '2018-02-15', '2018-02-16', '2018-02-17', '2018-02-18', '2018-02-19',
            '2018-02-20', '2018-02-21', '2018-02-22', '2018-02-23', '2018-02-24', '2018-02-25', '2018-02-26',
            '2018-02-27', '2018-02-28', '2018-03-01', '2018-03-02', '2018-03-03', '2018-03-04', '2018-03-05',
            '2018-03-06', '2018-03-07', '2018-03-08', '2018-03-09', '2018-03-10', '2018-03-11', '2018-03-12',
            '2018-03-13', '2018-03-14', '2018-03-15', '2018-03-16', '2018-03-17', '2018-03-18', '2018-03-19',
            '2018-03-20', '2018-03-21', '2018-03-22', '2018-03-23', '2018-03-24', '2018-03-25', '2018-03-26',
            '2018-03-27', '2018-03-28', '2018-03-29', '2018-03-30', '2018-03-31']

    x = [7.609366416931152, 4.418840408325195, 4.787491798400879, 4.9972124099731445, 4.584967613220215, 4.394449234008789, 5.998936653137207, 6.375024795532227, 4.8903489112854, 4.477336883544922, 4.983606815338135, 4.787491798400879, 4.304065227508545, 6.040254592895508, 7.587817192077637, 5.176149845123291, 4.477336883544922, 4.8903489112854, 4.934473991394043, 4.875197410583496, 5.849324703216553, 6.278521537780762, 4.8978400230407715, 5.2257466316223145, 4.875197410583496, 5.24174690246582, 4.7004804611206055, 6.115891933441162, 6.514712810516357, 4.744932174682617, 4.905274868011475, 4.955827236175537, 5.036952495574951, 4.770684719085693, 6.079933166503906, 6.388561248779297, 5.0434250831604, 5.105945587158203, 5.1704840660095215, 4.682131290435791, 5.135798454284668, 6.0450053215026855, 6.398594856262207, 4.72738790512085, 4.007333278656006,
         4.543294906616211, 5.023880481719971, 4.762174129486084, 6.03308629989624, 7.585280895233154, 4.8978400230407715, 4.465908050537109, 4.653960227966309, 4.394449234008789, 4.934473991394043, 5.828945636749268, 6.548219203948975, 4.969813346862793, 4.9904327392578125, 4.595119953155518, 4.787491798400879, 4.564348220825195, 5.746203422546387, 6.513230323791504, 4.976733684539795, 4.510859489440918, 5.003946304321289, 4.430816650390625, 3.828641414642334, 5.902633190155029, 6.473890781402588, 4.779123306274414, 4.8903489112854, 4.905274868011475, 5.075173854827881, 5.135798454284668, 6.073044300079346, 6.7405195236206055, 5.111987590789795, 4.691348075866699, 4.465908050537109, 5.075173854827881, 4.770684719085693, 6.154858112335205, 6.546785354614258, 4.7004804611206055, 4.174387454986572, 5.068904399871826, 4.543294906616211, 5.817111015319824]

    full_record = np.round(np.expm1(x))

    response = _predict(URL, model_stats=model_stats, day_list=days, uckey='magazinelock,1,3G,g_f,2,pt,1004,icc,2,11',
                        age='2', si='1', network='3G', gender='g_f',
                        media='', ip_location='', records_hour_price_list=[(full_record, 11, 2)])

    # pred = response[0][0]
    # true = full_record[79 :89]
    # error = np.average(np.divide(np.abs(np.subtract(pred,true)), true))
    # print(response[0][0],true, error)
