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
This script generates sample user documents and pushes to elasticsearch.
Each document is called user-doc.

A user-doc contains all the profile driven, location driven and device driven features. 

The script generates <num_of_user> number of user-docs.

Each user-doc has both multi-value and single-value features.
The values of these features reflect different activates of the user for a specific day. 
For example, "dm" : "mate20-mate10-mate8" shows that the user used 3 different phone models on "2018-01-02".


 "2018-01-02" : {
              "a" : "4",
              "dm" : "mate20-mate10-mate8",
              "g" : "g_m",
              "ai" : "magazine",
              "m" : "cloudFolder-magazinelock",
              "ipl" : "7685432-100000-1243278",
              "si" : "6-4-1",
              "r" : "1243278",
              "au" : "_active_30days-wifi_installed_not_activated-shooping-food-travel_installed_not_activated-travel",
              "t" : "3G-4G",
              "dpc" : "3600_4500",
              "pda" : "353",
              "pm" : "CPD-CPT"

IMS Service contains the logic to calculate unique number of users that match specific criteria.

"""

es_host = "10.10.10.10"
es_port = 9200
es_index = 'ulp_test_10082019'
es_type = 'doc'

template1 = {
    "a": "1-2-3-4",
    "g": "g_m-g_f-g_x",
    "m": "cloudFolder-magazinelock",
    "si": "1-0-3-2-5-4-7-6-9-8",
    "r": "100000-1243278-7685432",
    "ipl": "100000-1243278-7685432",
    "t": "3G-4G",
    "dpc": "2500_3500-3600_4500",
    "pm": "CPD-CPC-CPT-CPM",
    "dm": "mate8-mate10-mate20",
    "ai": "game-magazine-movie",
    "au": "wifi-wifi_installed_not_activated-_active_30days-life-life_installed_not_activated-shooping-shooping_activated-travel-travel_installed_not_activated-car-car_installed_not_activated-food-food_installed_not_activated",
    "pda": "288-354-353"
}


# This generates a list of unique imeis
def generate_imei(size_of_imeis):
    imeis = set()
    for _ in range(size_of_imeis):
        imei = ''
        for _ in range(10):
            r = random.randint(1, 9)
            imei += str(r)
        imeis.add(imei)
    return list(imeis)


# This generate a random document based on template
def generate_random_doc_from_template():
    new_doc = {}
    for key, value in template1.items():
        vlist = value.split('-')
        sample_size = random.randint(1, len(vlist))
        new_value = '-'.join(random.sample(vlist, sample_size))
        new_doc[key] = new_value
    return new_doc


# Generates data for 1 imei
def user_doc_generator(imei):
    one_doc = {'_index': es_index, '_type': es_type, 'imei': imei, 'days': {}}
    days = ['2018-01-0' + str(dt) for dt in range(1, 8)]
    for day in days:
        indoc = generate_random_doc_from_template()
        one_doc['days'][day] = indoc
    return one_doc


if __name__ == "__main__":
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    num_of_user = 100
    imeis = generate_imei(num_of_user)
    actions = [user_doc_generator(imei) for imei in imeis]
    helpers.bulk(es, actions)
