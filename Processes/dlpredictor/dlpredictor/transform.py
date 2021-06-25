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

import json
import math
import statistics

from dlpredictor.prediction.ims_predictor_util import convert_records_map_to_list
from dlpredictor.util.sparkesutil import *

"""
serving_url = url of the predictor tf serving server
forcaster = forcaster object
model_stats = statistics of model coming from es
cfg = config file
uckey = uckey string
day_hour_counts = map(day,hour_counts)
    hour_counts = [hour_count]
    hour_count = map(hour, count_array)
"""


def predict_counts_for_uckey(serving_url, forecaster, model_stats, cfg):
    def _helper(uckey, day_hour_counts):
        ucdoc = convert_day_hour_counts_to_ucdoc(uckey, day_hour_counts)
        records = convert_records_map_to_list(ucdoc.records)
        # records is a list of ucdays
        ucdoc.predictions = forecaster.dl_forecast(
            serving_url=serving_url, model_stats=model_stats, ucdoc=ucdoc, ucday_list=records)
        ucdoc.records = None
        ucdoc.bookings = []
        return json.dumps(ucdoc, default=lambda x: x.__dict__)
    return _helper


def replace_with_median(ts):
    ts_minus_0 = [i for i in ts if i != 0]
    median = 0
    if len(ts_minus_0) > 0:
        median = statistics.median(ts_minus_0)
    ts = [i if i != 0 else median for i in ts]
    return ts


def normalize_ts(ts):
    ts_n = [math.log(i + 1) for i in ts]
    return ts_n

def predict_daily_uckey(days, serving_url, forecaster, model_stats, columns):

    def _denoise(ts):
        non_zero_ts = [_ for _ in ts if _ != 0]
        nonzero_p = 0.0
        if len(non_zero_ts) > 0:
            nonzero_p = 1.0 * sum(ts) / len(non_zero_ts)

        return [i if i > (nonzero_p / 10.0) else 0 for i in ts]

    def _helper(cols):
        day_list = days[:]
        ucdoc_attribute_map = {}
        for feature in columns:
            ucdoc_attribute_map[feature] = cols[feature]

        # determine ts_n and days
        model_input_ts = []

        # ts = {u'2019-11-02': [u'1:862', u'3:49', u'2:1154'], u'2019-11-03': [u'1:596', u'3:67', u'2:1024']}
        ts = ucdoc_attribute_map['ts'][0]
        price_cat = ucdoc_attribute_map['price_cat']

        for day in day_list:
            imp = 0.0
            if day in ts:
                count_array = ts[day]
                for i in count_array:
                    parts = i.split(':')
                    if parts[0] == price_cat:
                        imp = float(parts[1])
                        break
            model_input_ts.append(imp)

        # remove science 06/21/2021
        # model_input_ts = replace_with_median(model_input_ts)

        model_input_ts = _denoise(model_input_ts)

        ts_n = normalize_ts(model_input_ts)
        ucdoc_attribute_map['ts_n'] = ts_n

        # add page_ix
        page_ix = ucdoc_attribute_map['uckey']+'-'+price_cat
        ucdoc_attribute_map['page_ix'] = page_ix

        rs_ts, rs_days = forecaster.dl_daily_forecast(
            serving_url=serving_url, model_stats=model_stats, day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map)

        #respose = {'2019-11-02': 220.0, '2019-11-03': 305.0}

        response = {}
        for i, day in enumerate(rs_days):
            response[day] = rs_ts[i]
        return response

    return _helper


def build_count_array_from_price_count_map(price_count_map, multipliyer):
    count_array = []

    # price_cat can be different names, should come from config
    for price_cat in ['0', '1', '2', '3']:
        count = 0.0
        if price_cat in price_count_map:
            count = float(price_count_map[price_cat]*multipliyer)
        count_array.append(price_cat+':'+str(count))
    return count_array


def generate_ucdoc(traffic_dist):
    def _helper(uckey, ucdoc_elements):

        # ucdoc_elements=[{price_cat=u'1', ratio=0.5007790923118591, day_prediction_map={u'2019-11-02': 220.0, u'2019-11-03': 305.0}}]

        # build day_hour_counts

        # day_hour_counts = map(day,hour_counts)
        # hour_counts = [hour_count]
        # hour_count = map(hour, count_array)

        # day_price_count_map are map<day, map<price_cat,impression>>
        # day_price_count_map={u'2019-11-02': {u'1': 220.0}, u'2019-11-03': {u'1': 200.0, u'2': 320.0}}

        day_price_count_map = {}
        for ucdoc_element in ucdoc_elements:
            price_cat = ucdoc_element['price_cat']
            ratio = ucdoc_element['ratio']
            day_prediction_map = ucdoc_element['day_prediction_map']
            for day, imp in day_prediction_map.items():
                if day not in day_price_count_map:
                    day_price_count_map[day] = {}
                day_price_count_map[day][price_cat] = imp * 1.0 * ratio

        day_hour_counts = []
        for day, price_count_map in day_price_count_map.items():
            hour_counts = []
            for hour_index in range(24):
                multipliyer = traffic_dist[hour_index] / 100.0
                count_array = build_count_array_from_price_count_map(
                    price_count_map=price_count_map, multipliyer=multipliyer)
                hour_counts.append({hour_index: count_array})
            day_hour_counts.append({day: hour_counts})

        # UCKey length is always 8
        while len(uckey.split(',')) < 8:
            uckey += ','

        ucdoc = convert_day_hour_counts_to_ucdoc(uckey, day_hour_counts)
        ucdoc.predictions = ucdoc.records
        ucdoc.records = None
        ucdoc.bookings = []
        return json.dumps(ucdoc, default=lambda x: x.__dict__)
    return _helper


def format_data(x, field_name):
    _doc = {'uckey': x[0], field_name: json.loads(x[1])}
    return (x[0], json.dumps(_doc))


if __name__ == '__main__':
    # [Row(cluster_uckey=u'1090', day_price_count_map={u'2019-11-02': {u'1': 220.0}, u'2019-11-03': {u'1': 200.0, u'2': 320.0}}, uckey=u'native,x0ej5xhk60kjwq,WIFI,g_m,1,CPM,57', ratio=0.19797085225582123, prediction_output=u'HI')]

    ucdoc_elements = [{'price_cat': '1', 'ratio': 0.5007790923118591, 'day_prediction_map': {
        u'2019-11-02': 220.0, u'2019-11-03': 305.0}}]
    uckey = 'native,x0ej5xhk60kjwq,WIFI,g_m,1,CPM,57'
    ratio = 0.19797085225582123
    traffic_dist = [2.905931696, 1.792490513, 1.592770122, 1.447972838, 1.657679249, 2.716197324, 5.117835031, 6.5308568, 6.570800879, 5.302576393, 4.423806671,
                    4.43379269, 4.858198522, 4.338925504, 4.219093269, 4.224086279, 4.613541043, 5.412422608, 5.60714999, 5.327541442, 5.167765129, 4.828240463, 4.009386858, 2.900938686]
    day_price_count_map = {u'2019-11-02': {u'1': 220.0},
                           u'2019-11-03': {u'1': 200.0, u'2': 320.0}}
    ucdos_json = generate_ucdoc(traffic_dist)(
        uckey=uckey, ucdoc_elements=ucdoc_elements)
    print(ucdos_json)
