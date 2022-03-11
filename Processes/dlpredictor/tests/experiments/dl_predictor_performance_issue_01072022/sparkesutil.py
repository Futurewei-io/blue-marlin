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

from imscommon_dl.model.ucdoc import UCDoc
from imscommon_dl.model.ucday import UCDay
from imscommon_dl.model.uchour import UCHour
from imscommon_dl.es.ims_esclient import ESClient
import yaml
import pickle


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


def get_model_stats(cfg, model_name, model_version):
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


def get_model_stats_using_pickel(cfg):
    model_config_path = cfg["config_path"]
    pkl_path = cfg["tf_statistics_path"]
    try:
        path = model_config_path + "/config_dl_model.yml"
        with open(path, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            try:
                with open(pkl_path, 'rb') as handle:
                    pk = pickle.load(handle)
                result = {'model': {
                    'name': cfg['trainer']['name'], 'version': cfg['save_model']['model_version'],
                    'duration': cfg['tfrecorder_reader']['duration'], 'train_window': cfg['save_model']['train_window'],
                    'predict_window': cfg['trainer']['predict_window']}, 'stats': pk['stats']}
                return result
            except Exception as e:
                print("pickel file path ", pkl_path, "   exception:  ", e)
            finally:
                handle.close()
    except Exception as e:
        print("dl config path: ", path, "   exception:  ", e)
    finally:
        ymlfile.close()
    return None


def sum_day_count_array(day_count_arrays):
    result_map = {}
    for day_count_array in day_count_arrays:
        for item in day_count_array:
            for day, v in item.items():
                if not day:
                    continue
                if day not in result_map:
                    result_map[day] = []
                result_map[day] = add_count_arrays(result_map[day], v)
    return [result_map]


def add_count_arrays(ca1, ca2):
    result_map = {}
    for i in ca1 + ca2:
        key, value = i.split(':')
        if key not in result_map:
            result_map[key] = 0
        result_map[key] += int(value)
    result = []
    for key, value in result_map.items():
        result.append(key + ":" + str(value))
    return result
