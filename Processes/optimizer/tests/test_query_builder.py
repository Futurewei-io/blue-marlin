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

import argparse
from optimizer.log import *
from imscommon.es.ims_esclient import ESClient
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import uuid
from pyspark.sql.functions import udf
import yaml
import logging
import sys
import json
from datetime import date, datetime
import optimizer.util
import optimizer.dao.query_builder
import optimizer.main


def test_1(cfg):
    es_client_predictions = ESClient(cfg['es_host'], cfg['es_port'],
                                     cfg['es_predictions_index'], cfg['es_predictions_type'])
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b6']
    ors = ['b7']
    day = cfg['today']
    day = optimizer.util.convert_date(day)
    query = optimizer.dao.query_builder.get_prediction_count(
        ands, ors, bookings_map, day, es_client_predictions)
    print(query)


def test_2(cfg):
    es_client_tbr = ESClient(cfg['es_host'], cfg['es_port'],
                             cfg['es_tbr_index'], cfg['es_tbr_type'])
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b1', 'b2']
    query = optimizer.dao.query_builder.get_tbr_ratio(
        ands, bookings_map, es_client_tbr)
    print(query)


def test_3(cfg):
    es_client_predictions = ESClient(cfg['es_host'], cfg['es_port'],
                                     cfg['es_predictions_index'], cfg['es_predictions_type'])
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b1', 'b2']
    day = cfg['today']
    (query, result) = optimizer.dao.query_builder.get_prediction_count(
        ands, [], bookings_map, day, es_client_predictions)
    print(query)
    print(result)


def test_4(cfg):
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings = optimizer.util.adjust_booking_dates(bookings)
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b1', 'b2']
    day = cfg['today']
    r = optimizer.main.get_bb_count(cfg, bookings_map)(ands, [], day)
    print(r)


def test_5(cfg):
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings = optimizer.util.adjust_booking_dates(bookings)
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b1', 'b2']
    day = cfg['today']

    df_day = day
    df_ands = ['b1']
    df_allocated = {}
    # inventory of bb (df row)
    df_amount = 4000
    booking = bookings[0]
    # total inventory of connected resources
    total_inventory = 10000
    h, _ = optimizer.algo.hwm.update_allocation_for_booking(
        None, day, booking, total_inventory)
    r = h(df_day, df_ands, df_allocated, df_amount)
    print(r)


def test_6(cfg):
    es_client_predictions = ESClient(cfg['es_host'], cfg['es_port'],
                                     cfg['es_predictions_index'], cfg['es_predictions_type'])
    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings_map = optimizer.util.get_bookings_map(bookings)
    ands = ['b1', 'b2']
    day = cfg['today']
    (query, result) = optimizer.dao.query_builder.get_ucdoc_prediction_count(
        ands, [], bookings_map, day, es_client_predictions)
    print(query)
    print(result)


def test_7(cfg):
    resources = {'magazinelock,3,5G,g_x,2,pt,1004,icc': 1000, 'minusonepage,1,5G,g_f,4,pt,1003,icc': 500,
                 'minusonepage,1,3G,g_f,1,pt,1003,icc': 300}

    demands = {'b1': 200, 'b2': 500}

    allocation_map = optimizer.algo.hwm.hwm_generic_allocation(
        resources, resources.keys(), demands, demands.keys())

    print(allocation_map)


def test_8(cfg):
    allocation_maps = [{'b2': 14, 'b3': 18, 'b1': 9}, {
        'b2': 14, 'b3': 18, 'b1': 9}, {'b2': 14, 'b1': 9}]
    result = optimizer.main.agg_allocation_maps(allocation_maps)
    print(result)


def test_9(cfg):
    es_client_bb = ESClient(cfg['es_host'], cfg['es_port'],
                            cfg['es_bb_index'], cfg['es_bb_type'])
    ands = ['b1', 'b2']
    minus = ['b3']
    day = cfg['today']
    allocated = {'b1': 100, 'b2': 50}
    result = optimizer.dao.query_builder.index_bb(
        day, ands, minus, allocated, es_client_bb)
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='optimizer')
    parser.add_argument('config_file')
    parser.add_argument('today', help='today %Y%m%d e.g. 20181230')
    args = parser.parse_args()

    schema = StructType([StructField('day', StringType(), True),
                         StructField('ands', ArrayType(StringType()), True),
                         StructField('minus', ArrayType(StringType()), True),
                         StructField('allocated', MapType(
                             StringType(), IntegerType()), True),
                         StructField('amount', IntegerType(), True)])

    # Load config file
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            cfg['today'] = args.today
    except Exception as e:
        print(e)

    test_9(cfg)
