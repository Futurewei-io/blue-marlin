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

from ims_esclient import ESClient
import time
import pandas as pd

"""
This file is to get the predicted value from Elastic Search.
This program reads the csv file which contains list of uckey and the price category and dumpt the predicted result for that in to a csv file.
Author: Faezeh
"""
# def agg(uckey, price_cat):
#     uckey_p = uckey + "-" + str(price_cat)
#     return uckey_p
# df = df.withColumn("uckey_pc" , udf(agg)(df.uckey, df.price_cat))

df = pd.read_csv('dense_true.csv', header = 0)

es_host = '10.213.37.41'
es_port = '9200'

# This is a correct index
es_index = 'dlpredictor_05062021_predictions'
es_type = 'doc'

# Load the prediction counts into a dictionary.
start_time = time.time()
es = ESClient(es_host, es_port, es_index, es_type)

uckey_list = []
for i in range(len(df)):
    record= (df['uckey'][i],df['price_cat'][i], df['uckey_pc'][i])
    uckey_list.append(record)

l = []
for row in uckey_list:
    uc = row[0]
    price_cat = row[1]
    uckey_pc = row[2]
    body = {
        "size": 1,
        "query": {"bool": {"must": [

            {"match": {
                "_id": uc
            }}

        ]}}

    }
    hits = es.search(body)
    es_records = {}
    uckeys = []
    predicted_days = set()
    for ucdoc in hits:
        uckey = ucdoc['uckey']
        uckeys.append(uckey)
        predictions = ucdoc['ucdoc']['predictions']
        # predictions = ["2020-06-01"]
        for day, hours in predictions.items():
            predicted_days.add(day)
            hour = -1
            h0 = h1 = h2 = h3 = 0
            for hour_doc in hours['hours']:
                hour += 1
                es_records[(uckey, day, hour, '0')] = hour_doc['h0']
                es_records[(uckey, day, hour, '1')] = hour_doc['h1']
                es_records[(uckey, day, hour, '2')] = hour_doc['h2']
                es_records[(uckey, day, hour, '3')] = hour_doc['h3']
                h0 += hour_doc['h0']
                h1 += hour_doc['h1']
                h2 += hour_doc['h2']
                h3 += hour_doc['h3']
                # print('h0: {} : {}  h1: {} : {}  h2: {} : {}  h3: {} : {}'.format(
                #     hour_doc['h0'], h0, hour_doc['h1'], h1, hour_doc['h2'], h2, hour_doc['h3'], h3))

            es_records[(uckey, day, '0')] = h0
            es_records[(uckey, day, '1')] = h1
            es_records[(uckey, day, '2')] = h2
            es_records[(uckey, day, '3')] = h3
            # es_records[(uckey, day)] = h0 + h1 + h2 + h3
            # print('daily: {}  {}  {}  {} : {}'.format(h0, h1, h2, h3, h0+h1+h2+h3))
            if price_cat ==0:
                l.append([uckey_pc, day, es_records[(uckey, day, '0')]])
            if price_cat ==1:
                l.append([uckey_pc, day, es_records[(uckey, day, '1')]])
            if price_cat ==2:
                l.append([uckey_pc, day, es_records[(uckey, day, '2')]])
            if price_cat ==3:
                l.append([uckey_pc, day, es_records[(uckey, day, '3')]])


x = pd.DataFrame(l)
x.to_csv("dense_predicted_value.csv")