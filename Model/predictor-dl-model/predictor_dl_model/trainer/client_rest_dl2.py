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
import datetime, math
import sys

# from predictor_dl_model.pipeline.util import get_dow

def get_start_end(records_len, train_window, forward_offset):
    start = records_len - train_window - forward_offset
    end = records_len - forward_offset
    return start, end

def get_dow(day_list):
    dow_list = []
    for day in day_list:
        dow = datetime.datetime.strptime(day, '%Y-%m-%d').weekday()
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
        return pd.Series(data=base_index[dates].fillna(-1).astype(np.int64).values, index=date_range)

    return [lag(pd.DateOffset(months =m)) for m in (1,2)]




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

    train_window = model_stats['model']['train_window']  # comes from cfg
    predict_window = model_stats['model']['predict_window']  # comes from cfg
    x_hits = np.log(np.add(hits, 1)).tolist()  # ln + 1
    full_record_exp = np.log(np.add(full_record, 1)).tolist()

    if len(day_list_cut) != train_window + predict_window:
        raise Exception('day_list_cut and train window + predicti_window do not match. {} {} {}'.format(
            len(day_list_cut), train_window, predict_window))

    dow = get_dow(day_list_cut)
    dow = [[dow[0][i], dow[1][i]] for i in range(train_window + predict_window)]
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

    duration = model_stats['model']['duration']

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
    pf_age = [ucdoc_attribute_map['a__n'], ucdoc_attribute_map['a_1_n'],ucdoc_attribute_map['a_2_n'], ucdoc_attribute_map['a_3_n'],
           ucdoc_attribute_map['a_4_n'], ucdoc_attribute_map['a_5_n'], ucdoc_attribute_map['a_6_n']]
    pf_si = ucdoc_attribute_map['si_vec_n']
    pf_network = [  ucdoc_attribute_map['t_2G_n'], ucdoc_attribute_map['t_3G_n'],ucdoc_attribute_map['t_4G_n'],
             ucdoc_attribute_map['t_UNKNOWN_n'],ucdoc_attribute_map['t_WIFI_n'], ]
    pf_gender = [ucdoc_attribute_map['g__n'], ucdoc_attribute_map['g_g_f_n'], ucdoc_attribute_map['g_g_m_n'], ucdoc_attribute_map['g_g_x_n']]
    pf_price_cat= [ucdoc_attribute_map['price_cat_1_n'], ucdoc_attribute_map['price_cat_2_n'],ucdoc_attribute_map['price_cat_3_n']]
    full_record = np.expm1(ucdoc_attribute_map['ts_n'])

    # days_list is sorted from past to present [2018-01-01, 2018-01-02, ...]
    prediction_results = []
    # for full_record, price_cat in records_hour_price_list:
    train_window = model_stats['model']['train_window']
    prediction_window = model_stats['model']['predict_window']
    start = len(full_record) - train_window - forward_offset
    end = len(full_record) - forward_offset
    hits = full_record[start:end]

    day_list_cut = day_list[start:end]
    predict_day = [datetime.datetime.strptime(day_list_cut[-1], "%Y-%m-%d").date() + datetime.timedelta(days=x + 1) for
                   x in range(prediction_window)]
    predict_day_list= [x.strftime("%Y-%m-%d") for x in predict_day]
    day_list_cut.extend(predict_day_list)

    body = {"instances": []}
    instance = get_predict_post_body(
        model_stats, day_list, day_list_cut, page_ix, pf_age, pf_si, pf_network, pf_gender, full_record, hits, pf_price_cat, predict_day_list, forward_offset)
    body['instances'].append(instance)

    body_json = json.dumps(body)
    result = requests.post(serving_url, data=body_json).json()
    predictions = result['predictions'][0]
    predictions = np.round(np.expm1(predictions))
    prediction_results.append(predictions.tolist())

    return prediction_results, predict_day_list



if __name__ == '__main__':  # record is equal to window size
    URL = "http://10.193.217.105:8508/v1/models/dl_20210706:predict"

    model_stats = {
        "model": {"days": ["2020-03-01", "2020-03-02", "2020-03-03", "2020-03-04", "2020-03-05", "2020-03-06", "2020-03-07",
              "2020-03-08", "2020-03-09", "2020-03-10", "2020-03-11", "2020-03-12", "2020-03-13", "2020-03-14",
              "2020-03-15", "2020-03-16", "2020-03-17", "2020-03-18", "2020-03-19", "2020-03-20", "2020-03-21",
              "2020-03-22", "2020-03-23", "2020-03-24", "2020-03-25", "2020-03-26", "2020-03-27", "2020-03-28",
              "2020-03-29", "2020-03-30", "2020-03-31", "2020-04-01", "2020-04-02", "2020-04-03", "2020-04-04",
              "2020-04-05", "2020-04-06", "2020-04-07", "2020-04-08", "2020-04-09", "2020-04-10", "2020-04-11",
              "2020-04-12", "2020-04-13", "2020-04-14", "2020-04-15", "2020-04-16", "2020-04-17", "2020-04-18",
              "2020-04-19", "2020-04-20", "2020-04-21", "2020-04-22", "2020-04-23", "2020-04-24", "2020-04-25",
              "2020-04-26", "2020-04-27", "2020-04-28", "2020-04-29", "2020-04-30", "2020-05-01", "2020-05-02",
              "2020-05-03", "2020-05-04", "2020-05-05", "2020-05-06", "2020-05-07", "2020-05-08", "2020-05-09",
              "2020-05-10", "2020-05-11", "2020-05-12", "2020-05-13", "2020-05-14", "2020-05-15", "2020-05-16",
              "2020-05-17", "2020-05-18", "2020-05-19", "2020-05-20", "2020-05-21", "2020-05-22", "2020-05-23",
              "2020-05-24", "2020-05-25", "2020-05-26", "2020-05-27", "2020-05-28", "2020-05-29", "2020-05-30",
              "2020-05-31", "2020-06-01", "2020-06-02", "2020-06-03", "2020-06-04", "2020-06-05", "2020-06-06",
              "2020-06-07", "2020-06-08", "2020-06-09", "2020-06-10", "2020-06-11", "2020-06-12", "2020-06-13",
              "2020-06-14", "2020-06-15", "2020-06-16", "2020-06-17", "2020-06-18", "2020-06-19", "2020-06-20",
              "2020-06-21", "2020-06-22", "2020-06-23", "2020-06-24", "2020-06-25", "2020-06-26", "2020-06-27",
              "2020-06-28", "2020-06-29", "2020-06-30"], "duration": 112,
     "holidays_norm": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
     "name": "model_dlpm_06242021_1635", "predict_window": 10, "train_window": 60, "version": "version_06242021_1635"},
        "stats": {"a_":[0.04429662466026579,0.20533941439740103],"a_1":[0.0594024062498474,0.23536391091868636],
                  "a_2":[0.13875638124033587,0.3443551863453624],"a_3":[0.18247821941727763,0.3847745538180887],
                  "a_4":[0.23540840480301564,0.4227143144991038],"a_5":[0.20678420677160178,0.40351830940626193],
                  "a_6":[0.1328737568768233,0.338151429839641],"g_":[0.03802539182225981,0.1910382838856348],
                  "g_g_f":[0.31290878405353795,0.46186909160791834],"g_g_m":[0.6370381966764169,0.4790592824120438],
                  "g_g_x":[0.012027627461027505,0.10846109268711399],
                  "holiday_stats":[0.0,0.0],"ipl_":[1.0,1.0],"ipl_1":[0.0,1.0],"ipl_10":[0.0,1.0],"ipl_11":[0.0,1.0],"ipl_12":[0.0,1.0],"ipl_13":[0.0,1.0],"ipl_14":[0.0,1.0],"ipl_15":[0.0,1.0],"ipl_16":[0.0,1.0],"ipl_17":[0.0,1.0],"ipl_18":[0.0,1.0],"ipl_19":[0.0,1.0],"ipl_2":[0.0,1.0],"ipl_20":[0.0,1.0],"ipl_21":[0.0,1.0],"ipl_22":[0.0,1.0],"ipl_23":[0.0,1.0],"ipl_24":[0.0,1.0],"ipl_25":[0.0,1.0],"ipl_26":[0.0,1.0],"ipl_27":[0.0,1.0],"ipl_28":[0.0,1.0],"ipl_29":[0.0,1.0],"ipl_3":[0.0,1.0],"ipl_30":[0.0,1.0],"ipl_31":[0.0,1.0],"ipl_32":[0.0,1.0],"ipl_33":[0.0,1.0],"ipl_34":[0.0,1.0],"ipl_35":[0.0,1.0],"ipl_36":[0.0,1.0],"ipl_37":[0.0,1.0],"ipl_38":[0.0,1.0],"ipl_39":[0.0,1.0],"ipl_4":[0.0,1.0],"ipl_40":[0.0,1.0],"ipl_41":[0.0,1.0],"ipl_42":[0.0,1.0],"ipl_43":[0.0,1.0],"ipl_44":[0.0,1.0],"ipl_45":[0.0,1.0],"ipl_46":[0.0,1.0],"ipl_47":[0.0,1.0],"ipl_48":[0.0,1.0],"ipl_49":[0.0,1.0],"ipl_5":[0.0,1.0],"ipl_50":[0.0,1.0],"ipl_51":[0.0,1.0],"ipl_52":[0.0,1.0],"ipl_53":[0.0,1.0],"ipl_54":[0.0,1.0],"ipl_55":[0.0,1.0],"ipl_56":[0.0,1.0],"ipl_57":[0.0,1.0],"ipl_58":[0.0,1.0],"ipl_59":[0.0,1.0],"ipl_6":[0.0,1.0],"ipl_60":[0.0,1.0],"ipl_61":[0.0,1.0],"ipl_62":[0.0,1.0],"ipl_63":[0.0,1.0],"ipl_64":[0.0,1.0],"ipl_65":[0.0,1.0],"ipl_66":[0.0,1.0],"ipl_67":[0.0,1.0],"ipl_68":[0.0,1.0],"ipl_69":[0.0,1.0],"ipl_7":[0.0,1.0],"ipl_70":[0.0,1.0],"ipl_71":[0.0,1.0],"ipl_72":[0.0,1.0],"ipl_73":[0.0,1.0],"ipl_74":[0.0,1.0],"ipl_75":[0.0,1.0],"ipl_76":[0.0,1.0],"ipl_77":[0.0,1.0],"ipl_78":[0.0,1.0],"ipl_79":[0.0,1.0],"ipl_8":[0.0,1.0],"ipl_80":[0.0,1.0],"ipl_81":[0.0,1.0],"ipl_82":[0.0,1.0],"ipl_9":[0.0,1.0],
                  "page_popularity":[577.1606531140158,5127.251109603019],
                  "price_cat_1":[0.6813400284793525,0.4659575570248394],"price_cat_2":[0.1969572060256314,0.3977003132674967],
                  "price_cat_3":[0.12170276549501612,0.3269426904032709],"r_":[0.06428884716597416,0.2445368641073571],"r_1":[0.017636910330801243,0.13112898948090662],"r_10":[0.007311564935032408,0.08479574334903354],"r_11":[0.0123057065964087,0.10979549743303678],"r_12":[0.010682300938137547,0.10238183510308607],"r_13":[0.012839921663653402,0.11211387396512836],"r_14":[0.0043072282145927634,0.0652208481532438],"r_15":[0.005510994725533859,0.07372435219261837],"r_16":[0.009430895699953106,0.09624349588546878],"r_17":[0.003106284001102974,0.05540235515012807],"r_18":[0.013727307649070212,0.11587668554591014],"r_19":[0.003658714691227056,0.060094360148970564],"r_2":[0.006930426840847276,0.08259001749726591],"r_20":[0.009621269428012426,0.0972004049920124],"r_21":[0.010746271555183338,0.10268192290785572],"r_22":[0.024583268037287404,0.15426934195117514],"r_23":[0.005721017802866344,0.07509957327880841],"r_24":[0.01830177624998661,0.13353918416581523],"r_25":[0.008662228002827519,0.0922767229097513],"r_26":[0.007283668595702679,0.08465678118091494],"r_27":[0.008913427277260215,0.0935936727765952],"r_28":[0.019489996624824472,0.1377225070294911],"r_29":[0.01184853904442632,0.10780878967593792],"r_3":[0.010360907737132814,0.10090031978343518],"r_30":[0.005268162614988744,0.0721404364566014],"r_31":[0.006795651437444575,0.08178152977272346],"r_32":[0.005624180821619731,0.07448664866729614],"r_33":[0.006411867702310304,0.07948751561735418],"r_34":[0.006509134818396776,0.0801375874910104],"r_35":[0.005580401140435336,0.07418513099650123],"r_36":[1.5449449320791486E-4,0.012364118289365515],"r_37":[0.0028466672077495808,0.05301062239853139],"r_38":[0.010713946790356105,0.10256955449886415],"r_39":[0.0034429989328785483,0.0583888083763328],"r_4":[0.013300424262355068,0.1141386154412099],"r_40":[0.021875249575959654,0.14573846520038694],"r_41":[4.0226892652173624E-4,0.019964317590957556],"r_42":[0.0022602156752338396,0.04726992138193806],"r_43":[0.01415631424810564,0.11771037667309092],"r_44":[0.004017545432034864,0.06299519948122188],"r_45":[0.012573840249382363,0.11103012675446208],"r_46":[0.012097359124970612,0.10888519798168067],"r_47":[0.008092052377949565,0.08924037498833616],"r_48":[0.0035848019436715956,0.05947222917426611],"r_49":[0.008673773828736435,0.0923572938291937],"r_5":[0.012552607682583329,0.11088179550318002],"r_50":[0.006951285377653645,0.08273353620921826],"r_51":[0.012508691815441772,0.11067069528262007],"r_52":[0.0025252799345467265,0.050031446895071115],"r_53":[0.006971198096591135,0.08284058885334963],"r_54":[0.013010404494975019,0.11285232518803595],"r_55":[0.011205366011483526,0.10480265985095695],"r_56":[0.004091688939927208,0.06358320428852193],"r_57":[0.009483973684679932,0.09654761327441443],"r_58":[0.0033479429826094058,0.05754055560727182],"r_59":[0.004110291414764129,0.0637456977480596],"r_6":[0.017170719926898895,0.1293825607377681],"r_60":[0.012502322863654236,0.11068315020688722],"r_61":[0.0032739755477089547,0.05691437647921438],"r_62":[0.004530736628742831,0.06688532080007667],"r_63":[0.008843517281097744,0.09324513541404153],"r_64":[0.00608604133973962,0.07746758538416228],"r_65":[0.007703450057748544,0.08708185205112809],"r_66":[0.0038174901754371078,0.061442386487589006],"r_67":[0.007226517945388782,0.08432460198736601],"r_68":[0.008491919780194587,0.09137931662767898],"r_69":[0.010766670130327805,0.1027959300414319],"r_7":[0.008717083083441964,0.09258227788620865],"r_70":[0.019928163154919562,0.13925707765065126],"r_71":[0.042367721440680677,0.20075663714068437],"r_72":[0.03365624054137039,0.17972947649606252],"r_73":[0.016191367690641226,0.12569029295661116],"r_74":[0.029418237267252897,0.1683753748704679],"r_75":[0.016422084256369943,0.12665559500401802],"r_76":[0.036994754532839284,0.18815474260948475],"r_77":[0.019072681048734642,0.1362788171295267],"r_78":[0.03046848772777889,0.17129000529130292],"r_79":[0.006810037790196545,0.08190570131006866],"r_8":[0.01711773510902206,0.129225322349193],"r_80":[0.04086274840775549,0.19733000594126685],"r_81":[0.034348686325118755,0.18150326085652782],"r_82":[0.0,1.0],"r_9":[0.008801054123012632,0.09300816483722156],
                  "si_15e9ddce941b11e5bdec00163e291137":[0.0021734242674061304,0.046569378305571674],
                  "si_17dd6d8098bf11e5bdec00163e291137":[8.064153488720677E-4,0.028386043378890852],
                  "si_5cd1c663263511e6af7500163e291137":[0.024480251817432363,0.15453491734752325],"si_66bcd2720e5011e79bc8fa163e05184e":[0.0747118339204077,0.2629261931283321],"si_68bcd2720e5011e79bc8fa163e05184e":[0.00832196657423368,0.09084457158709529],"si_71bcd2720e5011e79bc8fa163e05184e":[0.0031866896500037474,0.056360839080046306],"si_7b0d7b55ab0c11e68b7900163e3e481d":[0.07978115865997153,0.270954508123256],"si_a290af82884e11e5bdec00163e291137":[0.08873866446826051,0.28436658790013714],"si_a47eavw7ex":[0.21830173124484747,0.41309393235589104],"si_a8syykhszz":[0.004017087611481676,0.06325316286791836],"si_b6le0s4qo8":[0.0949651502660571,0.2931672358147947],"si_d4d7362e879511e5bdec00163e291137":[0.004023083264633141,0.06330015859143233],"si_d971z9825e":[0.0178340702990332,0.13234828576803975],"si_d9jucwkpr3":[0.024657123585400585,0.1550781155975643],"si_e351de37263311e6af7500163e291137":[0.04452971595593195,0.2062691152382904],"si_f1iprgyl13":[0.05222513677583752,0.22248105593614842],"si_j1430itab9wj3b":[0.003750281046241475,0.0611246892742874],"si_k4werqx13k":[0.004313872442479202,0.06553835383591226],"si_l03493p0r3":[0.022855429813385297,0.1494427184317444],"si_l2d4ec6csv":[0.01941692273102001,0.13798537212757175],"si_p7gsrebd4m":[0.00981188638237278,0.09856795825116378],"si_s4z85pd1h8":[0.003141722251367758,0.055963034419515],"si_w3wx3nv9ow5i97":[0.038240275800044965,0.1917760865203199],"si_w9fmyd5r0i":[0.023203177696170276,0.15054852435771146],"si_x0ej5xhk60kjwq":[0.10203102750505884,0.3026895630759413],"si_x2fpfbm8rt":[0.012063254140747957,0.10916852919996962],"si_z041bf6g4s":[0.01841864648130106,0.13445986071004987],"t_2g":[7.974871208755584E-4,0.028016668863524278],"t_3g":[0.009603467454533807,0.09696414375152526],"t_4g":[0.41960214977751586,0.49168826001768395],"t_unknown":[2.9121931074971E-5,0.00519491843658891],"t_wifi":[0.5699677615611796,0.49327218394896505]}
    }
    days = model_stats['model']['days']

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

    days = days[:-10]
    x = x[:-10]
    si = [-0.04667067527770996, -0.2841551601886749, -0.29444485902786255, -0.06350809335708618, -0.1994006484746933,
            -0.11050120741128922, -0.028408868238329887, -0.15841242671012878, -0.09160664677619934,
            -0.056540846824645996, -0.3120572865009308, -0.5284554362297058, -0.32392826676368713, -0.06355565786361694,
            -0.13475105166435242, 6.289364814758301, -0.21588164567947388, -0.23473970592021942, -0.06135460361838341,
            -0.06582210212945938, -0.15293772518634796, -0.1407172530889511, -0.09954438358545303, -0.05613924190402031,
            -0.15412424504756927, -0.3370814323425293, -0.1369824856519699]

    forward_offset = 0

    response = predict(serving_url = URL, model_stats=model_stats, day_list=days, ucdoc_attribute_map = {'uckey': 'magazinelock,1,3G,g_f,2,pt,1004,icc',
    'ts': [], 'price_cat': '1',
    'p':462.7049255371094 , 'a__n': 4.654261589050293, 'a_1_n': -0.25238537788391113, 'a_2_n':-0.40294551849365234,
    'a_3_n': -0.4742470979690552, 'a_4_n': -0.5568971633911133, 'a_5_n':-0.5124530792236328, 'a_6_n':-0.39294159412384033,
    't_UNKNOWN_n': -0.0056058494374156, 't_3G_n': -0.09904143214225769, 't_4G_n': -0.853390634059906, 't_WIFI_n': 0.8717950582504272,
    't_2G_n':-0.02846473827958107, 'g__n': 5.035506725311279,
    'g_g_f_n': -0.6774836778640747, 'g_g_m_n':-1.3297690153121948, 'g_g_x_n':-0.11089347302913666 ,
    'price_cat_1_n':0.6838819980621338  , 'price_cat_2_n': -0.49524027, 'price_cat_3_n': -0.37224495,
    'si_vec_n': si, 'r_vec_n': [], 'p_n': -0.02232302 , 'ts_n': x, 'page_ix':'native,d9jucwkpr3,WIFI,,,CPM,,60-1'}, forward_offset = forward_offset )

    print(response)