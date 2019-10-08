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

import hashlib
import random

from elasticsearch import Elasticsearch
from elasticsearch import helpers

"""
This script generates sample tbr documents and pushes to elasticsearch.
Each document is called tbr-doc.

A tbr-doc contains traffic-browse values. These values are used to calculate tbr-ratio which 
explains how traffic is sliced by multi-value traffic features. 

A multi-value traffic feature is a feature that can be mapped to many possible values. This feature is not directly
presented in traffic but is derived from other single-value features. 
   
The script generates <num_of_generated_docs> number of tbr-docs.

Each tbr-doc has 2 parts:

The first part is mainly related to multi-value traffic features which includes the following fields:
(single-values might be used in tbrdoc to increase the accuracy of tbr system as they link tbr documents with ucdocs.)

"a" : age category [1, 2, 3, 4] (single-value)
"g" : gender [g_m, g_f, g_x] (single-value)
"dm" : device model
"ai" : app interest
"au" : app usage
"dpc" : device price category
"pda": audience 

The second part contains tbr values for each day.
 "days" : {
              "2018-01-02" : 635,
              "2018-01-03" : 351,
              "2018-01-01" : 482,
              "2018-01-06" : 470,
              "2018-01-07" : 492,
              "2018-01-04" : 163,
              "2018-01-05" : 371
            },
        
IMS Service contains the logic to calculate tbr ration based on tbr values and traffic criteria.

"""

es_host = "10.10.10.10"
es_port = 9200
es_index = 'tbr_test_10082019'
es_type = 'doc'

value_set = {
    "a": "1-2-3-4",
    "g": "g_m-g_f-g_x"
}

template1 = {
    "dpc": "2500_3500-3600_4500",
    "dm": "mate8-mate10-mate20",
    "ai": "game-magazine-movie",
    "au": "wifi-wifi_installed_not_activated-_active_30days-life-life_installed_not_activated-shooping-shooping_activated-travel-travel_installed_not_activated-car-car_installed_not_activated-food-food_installed_not_activated",
    "pda": "288-354-353"
}


def get_tbrkey(doc):
    attribute_list = ['ai', 'au', 'dm', 'pda', 'a', 'g']
    _tmp = []
    for attribute in attribute_list:
        if attribute in doc:
            _tmp.append(doc[attribute])
        else:
            _tmp.append('')
    tbrkey_id = ','.join(_tmp)
    tbrkey_id = hashlib.sha256(tbrkey_id).hexdigest()
    return tbrkey_id


# This method generates a random tbr document based on value_set and template1
def generate_random_tbr_doc():
    new_doc = {}
    for key, value in value_set.items():
        vlist = value.split('-')
        new_value = random.choice(vlist)
        new_doc[key] = new_value

    for key, value in template1.items():
        vlist = value.split('-')
        sample_size = random.randint(1, len(vlist))
        new_value = '-'.join(random.sample(vlist, sample_size))
        new_doc[key] = new_value

    new_doc['tbrKey_id'] = get_tbrkey(new_doc)
    return new_doc


# This method generates one prediction document
def tbr_doc_generator():
    tbr_doc = generate_random_tbr_doc()
    one_doc = {'_index': es_index, '_type': es_type, 'tbrkey_id': get_tbrkey(tbr_doc), 'data': tbr_doc}
    tbr_doc['days'] = {}
    days = ['2018-01-0' + str(dt) for dt in range(1, 8)]
    for day in days:
        tbr_doc['days'][day] = random.randint(100, 1000)
    return one_doc


if __name__ == "__main__":
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    num_of_generated_docs = 100
    actions = [tbr_doc_generator() for _ in range(num_of_generated_docs)]
    helpers.bulk(es, actions)
