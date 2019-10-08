# Copyright 2019, Futurewei Technologies
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import random

from elasticsearch import Elasticsearch
from elasticsearch import helpers

"""
This script generates sample predicted inventory documents and pushes to elasticsearch.
Each document is called uc-doc.
The script generates <num_of_generated_docs> number of uc-docs.

Each uc-doc has 2 parts:

The first part is related to traffic criteria which includes the following fields:
"a" : age category [1, 2, 3, 4]
"g" : gender [g_m, g_f, g_x]
"m" : media or channel
"ipl" : IP location
"r" : residence
"t" : network type
"pm": price model [CPD, CPC, CPT, CPM]

The second part contains predicted inventory amount.
  "days" : {
              "2018-01-02" : [
                {
                  "h2" : 800,
                  "h3" : 0,
                  "h0" : 0,
                  "h1" : 3700,
                  "total" : 4500
                },
                .
                .
                .
 The inventory data is hourly based hence each day has 24 sub-documents.
 Every inventory sub-document (h-doc) has the following fields:
 
 "h0" : amount of inventory with no price tag
 "h1" : amount of inventory with price tag of category 1
 "h2" : amount of inventory with price tag of category 2
 "h3" : amount of inventory with price tag of category 3
 "total" : amount of total inventory for that hour
                
"""

es_host = "10.10.10.10"
es_port = 9200
es_index = 'predictions_test_10082019'
es_type = 'doc'

value_set = {
    "a": "1-2-3-4",
    "g": "g_m-g_f-g_x",
    "m": "cloudFolder-magazinelock",
    "si": "1-0-3-2-5-4-7-6-9-8",
    "r": "100000-1243278-7685432",
    "t": "3G-4G",
    "pm": "CPD-CPC-CPT-CPM",
    "ipl": "100000-1243278-7685432"
}


def generate_random_count_doc():
    h1_count = random.randint(1, 100) * 100
    h2_count = random.randint(1, 100) * 100
    count_doc = {'h0': 0, 'h1': h1_count, 'h2': h2_count,
                 'h3': 0, 'total': h1_count + h2_count}
    return count_doc


def get_uckey(doc):
    attribute_list = ['m', 'si', 't', 'g', 'a', 'pm', 'r', 'ipl']
    _tmp = []
    for attribute in attribute_list:
        if attribute in doc:
            _tmp.append(doc[attribute])
        else:
            _tmp.append('')
    uckey = ','.join(_tmp)
    return uckey


# This method generates a random document based on value_set
def generate_random_doc_from_value_set():
    new_doc = {}
    for key, value in value_set.items():
        vlist = value.split('-')
        new_value = random.choice(vlist)
        new_doc[key] = new_value
    new_doc['uckey'] = get_uckey(new_doc)
    return new_doc


# This method generates one prediction document
def prediction_doc_generator():
    prediction_doc = generate_random_doc_from_value_set()
    one_doc = {'_index': es_index, '_type': es_type, 'uckey': prediction_doc['uckey'], 'ucdoc': prediction_doc}
    prediction_doc['predictions'] = {}
    days = ['2018-01-0' + str(dt) for dt in range(1, 8)]
    for day in days:
        prediction_doc['predictions'][day] = [generate_random_count_doc()
                                       for _ in range(24)]

    # For the first day we also generate prediction char records
    day = days[0]
    day_records = prediction_doc['predictions'][day]
    predictions_chart = {"hours": {}}
    i = 0
    for day_record in day_records:
        hr_key = 'hr' + str(i)
        i += 1
        predictions_chart['hours'][hr_key] = day_record
    prediction_doc['predictions_chart'] = predictions_chart

    return one_doc


if __name__ == "__main__":
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    num_of_generated_docs = 1000
    actions = [prediction_doc_generator() for _ in range(num_of_generated_docs)]
    helpers.bulk(es, actions)
