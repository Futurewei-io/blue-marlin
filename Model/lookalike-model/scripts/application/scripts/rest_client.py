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
THE script gets the data, process it and send the request to
the rest client and print out the response from the the rest API
"""
import requests
import json
import argparse
import yaml


def flatten(lst):
    f = [y for x in lst for y in x]
    return f


def str_to_intlist(table):
    ji = []
    for k in [table[j].split(",") for j in range(len(table))]:
        s = []
        for a in k:
            b = int(a.split(":")[0])
            s.append(b)
        ji.append(s)
    return ji


def inputData(record, keyword, length):
    if len(record['show_counts']) >= length:
        hist = flatten(record['show_counts'][:length])
        instance = {'hist_i': hist, 'u': record['did'], 'i': keyword, 'j': keyword, 'sl': len(hist)}
    else:
        hist = flatten(record['show_counts'])
        # [hist.extend([0]) for i in range(length - len(hist))]
        instance = {'hist_i': hist, 'u': record['did'], 'i': keyword, 'j': keyword, 'sl': len(hist)}
    return instance


def predict(serving_url, record, length, new_keyword):
    body = {"instances": []}
    for keyword in new_keyword:
        instance = inputData(record, keyword, length)
        body['instances'].append(instance)
    body_json = json.dumps(body)
    result = requests.post(serving_url, data=body_json).json()
    if 'error' in result.keys():
        predictions = result['error']
    else:
        predictions = result['predictions']
    return predictions


def run(cfg):
    length = cfg['input']['din_model_length']
    url = cfg['input']['din_model_tf_serving_url']
    ##time_interval, did, click_counts, show_counts, media_category, net_type_index, gender, age, keyword
    record = {"did": 0, "show_counts": ['25:3', '29:6,25:2', '29:1,25:2,14:2', '14:1,29:2,25:2',
                                          '29:1', '26:1,14:2,25:4', '14:1,25:3'], "show_clicks": [], "age": '10', "gender": '3'}
    record['show_counts'] = str_to_intlist(record['show_counts'])
    new_keyword = [26, 27, 29]
    response = predict(serving_url=url, record=record, length=length, new_keyword=new_keyword)

    print(response)


if __name__ == '__main__':  # record is equal to window size
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    run(cfg)
