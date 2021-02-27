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

from imscommon.model.ucdoc import UCDoc
from imscommon.model.ucday import UCDay
from imscommon.model.uchour import UCHour
from imscommon.es.ims_esclient import ESClient


def build_dict_from_counts(h, h_index):
    dict = {}
    dict['h_index'] = h_index
    h0 = {'h': 'h0', 't': 0}
    h1 = {'h': 'h1', 't': 0}
    h2 = {'h': 'h2', 't': 0}
    h3 = {'h': 'h3', 't': 0}

    for _ in h:
        s = _.split(':')
        index = int(s[0])
        count = float(s[1])
        if index == 0:
            h0['t'] = count
        elif index == 1:
            h1['t'] = count
        elif index == 2:
            h2['t'] = count
        elif index == 3:
            h3['t'] = count

    dict['h0'] = h0
    dict['h1'] = h1
    dict['h2'] = h2
    dict['h3'] = h3

    return dict


def convert_day_hour_counts_to_ucdoc(uckey, day_hour_counts):
    ucdoc = UCDoc.build_from_concat_string(uckey)
    for day_map in day_hour_counts:
        if len(day_map) > 0:
            # There is only one key here.
            day = next(iter(day_map.keys()))
            hour_count_map = day_map[day]
            for hour_map in hour_count_map:
                if len(hour_map) > 0:
                    hour = next(iter(hour_map.keys()))
                    h = hour_map[hour]
                    records = ucdoc.records
                    if day not in records:
                        records[day] = UCDay(str(day))
                    dict = build_dict_from_counts(h, hour)
                    uchour = UCHour.buildv1(dict)
                    records[day].hours[hour] = uchour
    return ucdoc


def convert_predictions_json_to_sorted_ucdays(predictions):
    ucdays = []
    days = predictions.keys()
    sorted_days = sorted(days)
    for day in sorted_days:
        ucday = UCDay(day)
        for i in range(0, 24):
            hour_json = predictions[day][i]
            ucday.hours[i] = UCHour.buildv2(hour_json)
        ucdays.append(ucday)
    return ucdays


def get_model_stats_from_es(cfg, model_name, model_version):
    '''
    [{'date': '2020-01-17', 'model': {'name': 's32', 'version': 1},
    'stats': {'g_g_m': [0.32095959595959594, 0.4668649491714752], 'g_g_f': [0.3654040404040404, 0.4815635452904544], 'g_g_x': [0.31363636363636366, 0.46398999646418304], 'a_1': [0.198989898989899, 0.3992572317838901], 'a_2': [0.2474747474747475, 0.4315630593164027], 'a_3': [0.295959595959596, 0.45649211860504146], 'a_4': [0.25757575757575757, 0.43731748751040456], 't_3G': [0.0, 1.0], 't_4G': [0.0, 1.0], 'si_1': [0.37424242424242427, 0.4839470491115894], 'si_2': [0.4042929292929293, 0.49077533664980666], 'si_3': [0.22146464646464648, 0.4152500106648333], 'price_cat_0': [0.0, 1.0], 'price_cat_1': [0.3333333333333333, 0.4714243623012701], 'price_cat_2': [0.3333333333333333, 0.47142436230126994], 'price_cat_3': [0.3333333333333333, 0.47142436230126994], 'holiday_stats': [0.044444444444444446, 0.20723493215097805]}}]
    '''
    es = ESClient(cfg['es_host'], cfg['es_port'],
                  cfg['es_model_index'], cfg['es_model_type'])
    body = {
        "query": {"bool": {"must": [
            {"match": {
                "model.name": model_name
            }},
            {"match": {
                "model.version": model_version
            }}
        ]}}
    }
    doc = es.search(body)
    if doc == None or len(doc) != 1:
        raise Exception(
            'model/version {}/{} not valid'.format(model_name, model_version))
    return doc[0]


def get_model_stats(hive_context, model_stat_table):
    '''
    return a dict
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
    '''
    command = """
            SELECT * FROM {}
            """.format(model_stat_table)
    df = hive_context.sql(command)
    rows = df.collect()
    if len(rows) != 1:
        raise Exception('Bad model stat table {} '.format(model_stat_table))
    model_info = rows[0]['model_info']
    model_stats = rows[0]['stats']
    result = {
        'model': model_info,
        'stats': model_stats
    }
    return result
