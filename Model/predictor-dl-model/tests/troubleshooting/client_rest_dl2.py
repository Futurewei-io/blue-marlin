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
from typing import List
import pandas as pd
import datetime
import math
import sys

# from predictor_dl_model.pipeline.util import get_dow

DATE_FORMAT = '%Y%m%d'


def get_start_end(records_len, train_window, forward_offset):
    start = records_len - train_window - forward_offset
    end = records_len - forward_offset
    return start, end


def get_dow(day_list):
    dow_list = []
    for day in day_list:
        dow = datetime.datetime.strptime(day, DATE_FORMAT).weekday()
        dow_list.append(dow)

    week_period = 7.0 / (2 * math.pi)
    sin_list = [math.sin(x / week_period) for x in dow_list]
    cos_list = [math.cos(x / week_period) for x in dow_list]
    return (sin_list, cos_list)


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

        '''
        packages in py38  gives error when offset not 0
        import pandas as pd
        import numpy as np
        date_range = pd.date_range('20200101','20200110')
        base_index = pd.Series(np.arange(0, len(date_range)), index=date_range)
        offset = 0
        base_index[date_range-offset].fillna(-1).astype(np.int64).values
        '''

        return pd.Series(data=base_index[dates].fillna(-1).astype(np.int64).values, index=date_range)

    # for by-passing error we set offset to 0 before was (30,60) 
    # we need 2 offsets
    return [lag(pd.DateOffset(days=m)) for m in (0,0)]


def make_pred_input(duration, train_window, predict_window, full_record_exp, x_hits, dow, lagged_ix, pf_age, pf_si,
                    pf_network,
                    pf_gender, page_ix, pf_price_cat,
                    page_popularity, quarter_autocorr, forward_offset):
    """
    Main method. Assembles input data into final tensors
    """
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
    cropped_lags = np.where(cropped_lags > len(
        full_record_exp) - 1, len(full_record_exp) - 1, cropped_lags)
    # Translate lag indexes to hit values
    lagged_hit = np.take(full_record_exp, cropped_lags)
    # Convert masked (see above) or NaN lagged hits to zeros
    lag_zeros = np.zeros_like(lagged_hit)
    lagged_hit = np.where(lag_mask | np.isnan(
        lagged_hit), lag_zeros, lagged_hit)
    start, end = get_start_end(duration, train_window, forward_offset)
    lagged_hit = lagged_hit[start:end + predict_window]
    norm_lagged_hits = np.divide(np.subtract(lagged_hit, mean), std)

    # stat = {'mean':mean, 'std':std}
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


def get_predict_post_body(model_stats, day_list, day_list_cut, page_ix, pf_age, pf_si, pf_network, pf_gender,
                          full_record, hits,  pf_price_cat, predict_day_list, forward_offset):

    train_window = model_stats['model_info']['train_window']  # comes from cfg
    # comes from cfg
    predict_window = model_stats['model_info']['predict_window']
    x_hits = np.log(np.add(hits, 1)).tolist()  # ln + 1
    full_record_exp = np.log(np.add(full_record, 1)).tolist()

    # TODO: Added
    duration = len(full_record_exp)

    if len(day_list_cut) != train_window + predict_window:
        raise Exception('day_list_cut and train window + predicti_window do not match. {} {} {}'.format(
            len(day_list_cut), train_window, predict_window))

    dow = get_dow(day_list_cut)
    dow = [[dow[0][i], dow[1][i]]
           for i in range(train_window + predict_window)]
    for x in predict_day_list:
        if x not in day_list:
            day_list.extend(predict_day_list)
    lagged_indx = np.stack(lag_indexes(day_list), axis=-1)

    # not used in the model (but we should keep it)
    page_popularity = np.mean(full_record)
    page_popularity = (
        page_popularity - model_stats['stats']['page_popularity'][0]) / \
        model_stats['stats']['page_popularity'][1]
    quarter_autocorr = 1

    # x_hits, x_features, norm_x_hits, x_lagged, y_features, mean, std, flat_ucdoc_features, page_ix
    truex, timex, normx, laggedx, timey, normmean, normstd, pgfeatures, pageix = make_pred_input(duration,
                                                                                                 train_window,
                                                                                                 predict_window,
                                                                                                 full_record_exp,
                                                                                                 x_hits, dow,
                                                                                                 lagged_indx, pf_age,
                                                                                                 pf_si, pf_network,
                                                                                                 pf_gender, page_ix,
                                                                                                 pf_price_cat,
                                                                                                 page_popularity,
                                                                                                 quarter_autocorr, forward_offset)

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


def predict(serving_url, model_stats, day_list, ucdoc_attribute_map, forward_offset):
    page_ix = ucdoc_attribute_map['page_ix']
    pf_age = [ucdoc_attribute_map['a__n'], ucdoc_attribute_map['a_1_n'], ucdoc_attribute_map['a_2_n'], ucdoc_attribute_map['a_3_n'],
              ucdoc_attribute_map['a_4_n'], ucdoc_attribute_map['a_5_n'], ucdoc_attribute_map['a_6_n']]
    pf_si = ucdoc_attribute_map['si_vec_n']
    pf_network = [ucdoc_attribute_map['t_2G_n'], ucdoc_attribute_map['t_3G_n'], ucdoc_attribute_map['t_4G_n'],
                  ucdoc_attribute_map['t_UNKNOWN_n'], ucdoc_attribute_map['t_WIFI_n'], ]
    pf_gender = [ucdoc_attribute_map['g__n'], ucdoc_attribute_map['g_g_f_n'],
                 ucdoc_attribute_map['g_g_m_n'], ucdoc_attribute_map['g_g_x_n']]
    pf_price_cat = [ucdoc_attribute_map['price_cat_1_n'],
                    ucdoc_attribute_map['price_cat_2_n'], ucdoc_attribute_map['price_cat_3_n']]
    full_record = np.expm1(ucdoc_attribute_map['ts_n'])

    # days_list is sorted from past to present [2018-01-01, 2018-01-02, ...]
    prediction_results = []
    # for full_record, price_cat in records_hour_price_list:
    train_window = model_stats['model_info']['train_window']
    prediction_window = model_stats['model_info']['predict_window']
    start = len(full_record) - train_window - forward_offset
    end = len(full_record) - forward_offset
    hits = full_record[start:end]

    day_list_cut = day_list[start:end]
    predict_day = [datetime.datetime.strptime(day_list_cut[-1], DATE_FORMAT).date() + datetime.timedelta(days=x + 1) for
                   x in range(prediction_window)]
    predict_day_list = [x.strftime(DATE_FORMAT) for x in predict_day]
    day_list_cut.extend(predict_day_list)

    body = {"instances": []}
    instance = get_predict_post_body(
        model_stats, day_list, day_list_cut, page_ix, pf_age, pf_si, pf_network, pf_gender, full_record, hits, pf_price_cat, predict_day_list, forward_offset)
    body['instances'].append(instance)

    body_json = json.dumps(body)
    result = requests.post(serving_url, data=body_json).json()
    print(result)
    predictions = result['predictions'][0]
    predictions = np.round(np.expm1(predictions))
    prediction_results.append(predictions.tolist())

    return prediction_results, predict_day_list


if __name__ == '__main__':  # record is equal to window size
    URL = "http://10.193.217.126:8501/v1/models/20220531:predict"

    model_stats = {
        "model_info": {
            "predict_window": 10,
            "train_window": 60,
            "name": "model_dlpm_05172022",
            "days": ["20220101", "20220102", "20220103", "20220104", "20220105", "20220106", "20220107", "20220108", "20220109", "20220110", "20220111", "20220112", "20220113", "20220114", "20220115", "20220116", "20220117", "20220118", "20220119", "20220120", "20220121", "20220122", "20220123", "20220124", "20220125", "20220126", "20220127", "20220128", "20220129", "20220130", "20220131", "20220201", "20220202", "20220203", "20220204", "20220205", "20220206", "20220207", "20220208", "20220209", "20220210", "20220211", "20220212", "20220213", "20220214", "20220215", "20220216", "20220217", "20220218", "20220219", "20220220", "20220221", "20220222", "20220223", "20220224", "20220225", "20220226", "20220227", "20220228", "20220301", "20220302", "20220303", "20220304", "20220305", "20220306", "20220307", "20220308", "20220309", "20220310", "20220311", "20220312", "20220313", "20220314", "20220315", "20220316", "20220317", "20220318", "20220319", "20220320"],
            "holidays_norm": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "version": "version_05172022",
            "duration": 79
        },
        "stats": {
            "a_6": [0.11803441518724754, 0.3206105136408193],
            "a_5": [0.15525707831567587, 0.36009701585663645],
            "a_4": [0.16964758819301692, 0.3733024160619906],
            "a_3": [0.16382623346701033, 0.36812758998926565],
            "a_2": [0.16160188505917003, 0.36618512872810904],
            "a_1": [0.04235599673371793, 0.19871854357052401],
            "g_g_x": [0.0, 1.0],
            "si_b6le0s4qo8": [0.05482697752662568, 0.22764356784639447],
            "page_popularity": [8886.694501133221, 22973.735023842688],
            "si_d4d7362e879511e5bdec00163e291137": [4.738718887348805e-05, 0.006883713002027608],
            "t_3G": [0.0023750939459957258, 0.04229944743745249],
            "g_g_m": [0.5067719345015185, 0.4969695453618017],
            "si_w9fmyd5r0i": [0.0228998590231131, 0.1495851615651056],
            "g_g_f": [0.4324921551157149, 0.49247343888536355],
            "g_": [0.06073591039259168, 0.23719685243168384],
            "si_71bcd2720e5011e79bc8fa163e05184e": [0.0576939024534717, 0.2331650920177939],
            "si_a290af82884e11e5bdec00163e291137": [0.052066673774745, 0.22216282310749794],
            "t_2G": [0.0011035163066091648, 0.0264055959637219],
            "si_s4z85pd1h8": [9.47743777469761e-05, 0.009734809615191435],
            "t_WIFI": [0.6751134555010421, 0.465828876320661],
            "si_j1430itab9wj3b": [1.1846797218372012e-05, 0.003441917665832815],
            "t_4G": [0.20035459462335434, 0.39719343972006366],
            "si_k4werqx13k": [0.004075298243119973, 0.06370822765074265],
            "a_": [0.1892768030788056, 0.38931645950272975],
            "si_d971z9825e": [0.028456006918529574, 0.16627263789430005],
            "si_66bcd2720e5011e79bc8fa163e05184e": [0.10593406072668254, 0.307755028475912],
            "si_e351de37263311e6af7500163e291137": [0.052647166838445225, 0.22332942827488161],
            "si_68bcd2720e5011e79bc8fa163e05184e": [0.09005935245406405, 0.2862684689768374],
            "si_l2d4ec6csv": [0.04128608830602647, 0.19895229613787355],
            "si_5cd1c663263511e6af7500163e291137": [0.003980523865372996, 0.06296607232818541],
            "si_17dd6d8098bf11e5bdec00163e291137": [2.3693594436744025e-05, 0.0048675778103909155],
            "si_a8syykhszz": [0.04619066235443248, 0.20989904003407786],
            "holiday_stats": [0.0, 0.0],
            "t_UNKNOWN": [0.001562388980041983, 0.02616128483345481],
            "si_z041bf6g4s": [0.03155986778974304, 0.17482621263063308],
            "price_cat_1": [1.0, 1.0],
            "price_cat_2": [0.0, 1.0],
            "price_cat_3": [0.0, 1.0],
            "si_x2fpfbm8rt": [0.005319211951049034, 0.07273912713556097],
            "si_l03493p0r3": [0.0787338143133004, 0.2693244513820377],
            "si_7b0d7b55ab0c11e68b7900163e3e481d": [0.06124794161898331, 0.2397859721197672],
            "si_x0ej5xhk60kjwq": [0.05646183554276101, 0.2308127548521334],
            "si_w3wx3nv9ow5i97": [0.058949662958619135, 0.2355318606893967],
            "si_f1iprgyl13": [0.03440309912215233, 0.1822633244583038],
            "si_d9jucwkpr3": [0.0006989610358839488, 0.026428786656111144],
            "si_a47eavw7ex": [0.11233133122460343, 0.3157755287282978]
        },
        # 25 SIs
        "si_list": ["a47eavw7ex", "66bcd2720e5011e79bc8fa163e05184e", "x0ej5xhk60kjwq", "l03493p0r3", "7b0d7b55ab0c11e68b7900163e3e481d", "b6le0s4qo8", "e351de37263311e6af7500163e291137", "a290af82884e11e5bdec00163e291137", "68bcd2720e5011e79bc8fa163e05184e", "f1iprgyl13", "w9fmyd5r0i", "w3wx3nv9ow5i97", "d971z9825e", "l2d4ec6csv", "z041bf6g4s", "71bcd2720e5011e79bc8fa163e05184e", "5cd1c663263511e6af7500163e291137", "x2fpfbm8rt", "d9jucwkpr3", "k4werqx13k", "j1430itab9wj3b", "a8syykhszz", "s4z85pd1h8", "17dd6d8098bf11e5bdec00163e291137", "d4d7362e879511e5bdec00163e291137"]
    }

    days = model_stats['model_info']['days']

    x = [
        4.919981, 4.912655, 4.912655, 5.1119876, 4.5217886, 5.638355, 5.673323, 5.874931, 5.6801724, 5.4026775,
        6.2422233, 6.150603, 5.918894, 6.251904, 6.0497336, 6.075346, 5.9939613, 6.222576, 6.1944056, 6.1025586,
        6.3261495, 5.811141, 5.3752785, 5.6903596, 5.4722705, 5.497168, 5.1357985, 4.919981, 5.117994, 4.9904327,
        5.0998664, 5.159055, 5.2983174, 4.7004805, 0.0, 4.1431346, 4.4998097, 4.6151204, 4.6728287, 4.356709,
        5.0238805, 5.749393, 5.4467373, 5.433722, 5.4764633, 4.9767337, 5.2832036, 5.996452, 6.011267, 6.089045,
        5.8318825, 5.556828, 5.590987, 5.755742, 5.8406415, 5.7525725, 5.733341, 4.4188404, 0.0, 4.454347, 3.988984,
        4.26268, 4.2904596, 4.804021, 4.4308167, 0.0, 6.0684257, 6.423247, 6.6795993, 6.6463904, 6.418365, 6.25575,
        6.1984787, 6.2841344, 6.350886, 6.4361506, 6.3733196, 6.668228, 6.2186003, 5.8230457, 6.2461066, 5.9839363,
        6.424869, 6.7730803, 6.510258, 6.591674, 6.767343, 6.666957, 5.529429, 5.075174, 4.248495, 0.0, 4.0943446,
        6.685861, 6.8330317, 6.7214255, 6.5971456, 6.629363, 6.72263, 6.5117455, 6.6795993, 6.8211074, 7.004882,
        6.9920964, 6.9641356, 6.991177, 6.6554403, 6.364751, 6.50129, 6.6346335, 6.43294, 6.9353704, 7.0030656,
        6.7968235, 7.2174435, 7.183871, 7.3225102, 7.010312, 7.4506607, 6.8855095, 6.163315, 0.0]

    predict_window = model_stats['model_info']['predict_window']
    days = days[:-predict_window]
    x = x[:len(days)]

    print("days:%s , x:%s", len(days), len(x))

    si = [-0.04667067527770996, -0.2841551601886749, -0.29444485902786255, -0.06350809335708618, -0.1994006484746933,
          -0.11050120741128922, -0.028408868238329887, -
          0.15841242671012878, -0.09160664677619934,
          -0.056540846824645996, -0.3120572865009308, -
          0.5284554362297058, -0.32392826676368713, -0.06355565786361694,
          -0.13475105166435242, 6.289364814758301, -0.21588164567947388, -
          0.23473970592021942, -0.06135460361838341,
          -0.06582210212945938, -0.15293772518634796, -
          0.1407172530889511, -0.09954438358545303, -0.05613924190402031,
          -0.15412424504756927]

    print("len(si)=", len(si))

    forward_offset = 0

    response = predict(serving_url=URL, model_stats=model_stats, day_list=days, ucdoc_attribute_map={'uckey': '11e79bc8fa163e05184e,4G,g_f,,,24',
                                                                                                     'ts': [], 'price_cat': '1',
                                                                                                     'p': 462.7049255371094, 'a__n': 4.654261589050293, 'a_1_n': -0.25238537788391113, 'a_2_n': -0.40294551849365234,
                                                                                                     'a_3_n': -0.4742470979690552, 'a_4_n': -0.5568971633911133, 'a_5_n': -0.5124530792236328, 'a_6_n': -0.39294159412384033,
                                                                                                     't_UNKNOWN_n': -0.0056058494374156, 't_3G_n': -0.09904143214225769, 't_4G_n': -0.853390634059906, 't_WIFI_n': 0.8717950582504272,
                                                                                                     't_2G_n': -0.02846473827958107, 'g__n': 5.035506725311279,
                                                                                                     'g_g_f_n': -0.6774836778640747, 'g_g_m_n': -1.3297690153121948, 'g_g_x_n': -0.11089347302913666,
                                                                                                     'price_cat_1_n': 0.6838819980621338, 'price_cat_2_n': -0.49524027, 'price_cat_3_n': -0.37224495,
                                                                                                     'si_vec_n': si, 'r_vec_n': [], 'p_n': -0.02232302, 'ts_n': x, 'page_ix': '11e79bc8fa163e05184e,4G,g_f,,,24-1'}, forward_offset=forward_offset)

    print(response)
