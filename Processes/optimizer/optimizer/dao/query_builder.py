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
import requests
from elasticsearch import Elasticsearch
import optimizer.util

PREDICTION_ATTR = ['a', 'g', 'm', 'ipl', 'si', 'r', 't']
TBR_ATTR = ['a', 'g', 'dm', 'ai', 'au', 'dpc', 'pds']
CORE_TBR_ATTR = ['a', 'g']
PREDICTIONS_H = 'h3'


def __get_ands_tbr_query(ands, bookings_map, tbr_attrs):
    tbr_query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                ]
            }
        },
        "aggs": {
            "alldays": {
                "sum": {
                    "script": {
                        "source":
                        """
                        int totals=0;
                        for(int entry : params._source.data.days.values()){
                            totals+=entry;
                        }
                        return totals;
                        """
                    }
                }
            }
        }
    }

    set_attr_values = set()
    for bk_id in ands:
        if bk_id not in bookings_map:
            continue
        bk_query = bookings_map[bk_id]['query']
        for attr in tbr_attrs:
            if attr in bk_query:
                value = bk_query[attr]
                if value:
                    attr_value_pair = attr + ':' + str(value)
                    if not attr_value_pair in set_attr_values:
                        set_attr_values.add(attr_value_pair)
                        # value is a list like ["g_m","g_f"] that is get converted to string "g_m-g_f"
                        match = {"match": {
                            "data." + attr: '-'.join(value)
                        }}
                        tbr_query['query']['bool']['must'].append(match)

    return tbr_query


def get_tbr_ratio(ands, bookings_map, es_client):
    query = __get_ands_tbr_query(ands, bookings_map, TBR_ATTR)
    result = es_client.aggregations(query)
    tbr_count = result['alldays']['value']

    if tbr_count == 0:
        return 0

    query = __get_ands_tbr_query(ands, bookings_map, CORE_TBR_ATTR)
    result = es_client.aggregations(query)
    total = result['alldays']['value']

    return tbr_count/total

# day format is YYYY-MM-DD
# this is to build the query for (p1 or p2 or p3) and (p4 and p5 and p6)
# if ors and empty it return (p4 and p5 and p6)


def __get_ands_ors_prediction_query(ands, ors, day, bookings_map):
    if(len(day) == 8):
        day = '-'.join([day[0:4], day[4:6], day[6:8]]) 
    prediction_query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                ]
            }
        },
        "aggs": {
            "day": {
                "sum": {
                    "field": "ucdoc.predictions.{}.{}".format(day, PREDICTIONS_H)
                }
            }
        }
    }

    set_attr_values = set()
    for bk_id in ands:
        if bk_id not in bookings_map:
            continue
        bk_query = bookings_map[bk_id]['query']
        for attr in PREDICTION_ATTR:
            if attr in bk_query:
                value = bk_query[attr]
                if value:
                    attr_value_pair = attr + ':' + str(value)
                    if not attr_value_pair in set_attr_values:
                        set_attr_values.add(attr_value_pair)
                        # value is a list like ["g_m","g_f"] that is get converted to string "g_m-g_f"
                        match = {"match": {
                            "ucdoc." + attr: '-'.join(value)
                        }}
                        prediction_query['query']['bool']['must'].append(match)

    # the following is to add ors
    should_term = {
        "bool": {
            "should": [
            ]
        }
    }
    set_attr_values = set()
    for bk_id in ors:
        if bk_id not in bookings_map:
            continue
        bk_query = bookings_map[bk_id]['query']
        for attr in PREDICTION_ATTR:
            if attr in bk_query:
                value = bk_query[attr]
                if value:
                    attr_value_pair = attr + ':' + str(value)
                    if not attr_value_pair in set_attr_values:
                        set_attr_values.add(attr_value_pair)
                        # value is a list like ["g_m","g_f"] that is get converted to string "g_m-g_f"
                        match = {"match": {
                            "ucdoc." + attr: '-'.join(value)
                        }}
                        should_term['bool']['should'].append(match)
    if len(should_term['bool']['should']) > 0:
        prediction_query['query']['bool']['must'].append(should_term)

    return prediction_query

# day format is YYYY-MM-DD
# returns a number


def __get_ands_minus_doc_process_query(ands, minus, day, bookings_map):
    prediction_query = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                ]
            }
        },
        "script_fields": {
            "total": {
                "script": {
                    "lang": "painless",
                    "source": "double totals;for(entry in params._source.ucdoc.predictions.entrySet()){if (entry.getKey()==params.day){for(item in entry.getValue()){totals += item['h0']+item['h1']+item['h2']+item['h3'];}}}return totals;",
                    "params": {"day": "{}".format(day)}
                }
            }
        }
    }

    for bk_id in ands:
        if bk_id not in bookings_map:
            continue
        bk_query = bookings_map[bk_id]['query']
        for attr in PREDICTION_ATTR:
            if attr in bk_query:
                value = bk_query[attr]
                if value:
                    # value is a list like ["g_m","g_f"] that is get converted to string "g_m-g_f"
                    match = {"match": {
                        "ucdoc." + attr: '-'.join(value)
                    }}
                    prediction_query['query']['bool']['must'].append(match)

    # the following is to add minus
    must_not_term = {
        "bool": {
            "must_not": [
            ]
        }
    }
    for bk_id in minus:
        if bk_id not in bookings_map:
            continue
        bk_query = bookings_map[bk_id]['query']
        for attr in PREDICTION_ATTR:
            if attr in bk_query:
                value = bk_query[attr]
                if value:
                    # value is a list like ["g_m","g_f"] that is get converted to string "g_m-g_f"
                    match = {"match": {
                        "ucdoc." + attr: '-'.join(value)
                    }}
                    must_not_term['bool']['must_not'].append(match)
    if len(must_not_term['bool']['must_not']) > 0:
        prediction_query['query']['bool']['must'].append(must_not_term)

    return prediction_query


def get_prediction_count(ands, ors, bookings_map, day, es_client):
    query = __get_ands_ors_prediction_query(
        ands, ors, day, bookings_map)
    result = es_client.aggregations(query)
    return (query, result['day']['value'])


def get_ucdoc_prediction_count(ands, minus, bookings_map, day, es_client):
    query = __get_ands_minus_doc_process_query(
        ands, minus, day, bookings_map)
    res = es_client.raw_search(query)
    result = {}
    for item in res['hits']['hits']:
        result[item['_id']] = round(item['fields']['total'][0])
    return (query, result)

# mark all the bbs on this day to be removed. days=[2018-11-07,2010-10-01]
# time_ms is integer in ms


def delete_bbs(days, time_ms, es_client_bb):
    new_days = [optimizer.util.convert_date_remove_dash(day) for day in days]
    day_str = '-'.join(new_days)
    query = {
        "script": {
            "source": "ctx._source.optimizer = params.delete",
            "lang": "painless",
            "params": {
                "delete": {"deleted": True, "time": time_ms}
            }
        },
        "query": {
            "bool": {
                "must": [{
                    "match": {
                        "day": day_str
                    }
                }]
            }
        }
    }
    res = es_client_bb.update_by_query(query)
    return res


def index_bb(day, ands, minus, allocated, es_client):
    bb_doc = {
        "and_bk_ids": ands,
        "allocated_amount": allocated,
        "minus_bk_ids": minus,
        "priority": 1,
        "day": optimizer.util.convert_date_remove_dash(day),
        "avg_tbr_map": {}
    }
    res = es_client.index(id=None, doc=bb_doc)
    return res


