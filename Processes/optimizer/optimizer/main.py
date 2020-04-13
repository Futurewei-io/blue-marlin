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

import argparse
from optimizer.log import *
from imscommon.es.ims_esclient import ESClient
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import uuid
from pyspark.sql.functions import udf, explode
import yaml
import logging
import sys
import json
from datetime import date, datetime
import time
import optimizer.util
from optimizer.dao.query_builder import get_prediction_count, get_tbr_ratio, get_ucdoc_prediction_count, delete_bbs
from optimizer.algo.hwm import hwm_allocation, hwm_generic_allocation

LOCK_BOOKING_ID = "lockBooking"


def calculate_avg_tbr(ands, minus, bookings_map, day, es_client_predictions, es_client_tbr):
    if (len(minus)) == 0:
        raise ValueError('len minus cannot be zero for this method!')

    numerator = 0
    denominator = 0
    for p in minus:
        new_ands = ands[:]
        new_ands.append(p)
        _, p = get_prediction_count(
            new_ands, [], bookings_map, day, es_client_predictions)
        t = get_tbr_ratio(new_ands, bookings_map, es_client_tbr)
        numerator += p * t
        denominator += p

    if numerator == 0:
        return 0

    if denominator == numerator:
        return 1

    return numerator/denominator


def get_bb_count(cfg, bookings_map):
    def helper(ands, minus, day):
        es_client_predictions = ESClient(cfg['es_host'], cfg['es_port'],
                                         cfg['es_predictions_index'], cfg['es_predictions_type'])
        es_client_tbr = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_tbr_index'], cfg['es_tbr_type'])

        # B4 and B3 - B2 - B1 = (p4 and p3).(t4 and t3) - (p4 and p3)and(p1 or p2) * avgTBR
        # avgTBR = [(p4 and p3 and p2)*(t4 and t3 and t2) + (p4 and p3 and p1)*(t4 and t3 and t1)] / [(p4 and p3 and p2) + (p4 and p3 and p1)]

        # if ands and minus has intersection then the result is 0
        if len(set(ands) & set(minus)) > 0:
            return 0

        _, p_and = get_prediction_count(
            ands, [], bookings_map, day, es_client_predictions)
        if p_and == 0:
            return 0

        t_and = get_tbr_ratio(ands, bookings_map, es_client_tbr)
        if t_and == 0:
            return 0

        p_ands_p_ors_tbr = 0
        if len(minus) > 0:
            _, p_ands_p_ors = get_prediction_count(
                ands, minus, bookings_map, day,   es_client_predictions)
            if p_ands_p_ors != 0:
                avg_tbr = calculate_avg_tbr(
                    ands, minus, bookings_map, day, es_client_predictions, es_client_tbr)
                p_ands_p_ors_tbr = p_ands_p_ors * avg_tbr

        return int(p_and * t_and - p_ands_p_ors_tbr)

    return helper


def bb_split_method(cfg, bookings_map, new_bk_id):
    def helper(x):
        new_ands = x.ands[:]
        if new_bk_id not in new_ands:
            new_ands.append(new_bk_id)
        new_minus = x.minus[:]
        if new_bk_id not in new_minus:
            new_minus.append(new_bk_id)
        x1 = (x.day, new_ands, x.minus[:], {}, get_bb_count(
            cfg, bookings_map)(new_ands, x.minus, x.day))
        x2 = (x.day, x.ands[:], new_minus, {}, get_bb_count(
            cfg, bookings_map)(x.ands, new_minus, x.day))

        result = []
        # x[4] is the value of get_bb_count()
        if x1[4] != 0:
            result.append(x1)

        if x2[4] != 0:
            result.append(x2)

        return result

    return helper


def split_bbs(cfg, df, bookings_map, new_bk_id, hive_context):
    rdd = df.rdd
    rdd = rdd.flatMap(bb_split_method(cfg, bookings_map, new_bk_id))
    # The cache method prevent the createDataFrame to through error messages in log, although the operation is fine~
    rdd.cache()
    df = hive_context.createDataFrame(rdd, optimizer.util.get_common_pyspark_schema())
    return df


def create_bb_df(cfg, bookings_map, day, booking, added_bk_ids, hive_context):
    ands = [booking['bk_id']]
    minus = added_bk_ids[:]
    inventory_amount = get_bb_count(
        cfg, bookings_map)(ands, minus, day)
    new_row = (day, ands, minus, {}, inventory_amount)
    rows = []
    if inventory_amount > 0:
        rows.append(new_row)
    # hive_context and schema are global varibales
    df = hive_context.createDataFrame(rows, optimizer.util.get_common_pyspark_schema())
    return df


def add_booking_to_bb_df(cfg, df, bookings_map, day, booking, added_bk_ids, hive_context):
    df = split_bbs(cfg, df, bookings_map, booking['bk_id'], hive_context)

    # add booking to bb_df
    new_df = create_bb_df(cfg, bookings_map, day, booking, added_bk_ids, hive_context)
    df = df.union(new_df)
    return df


def add_ucdoc_bb_allocation_map(cfg, df, bookings_map):
    def helper(ands, minus, amount, day, allocated):
        es_client_predictions = ESClient(cfg['es_host'], cfg['es_port'],
                                         cfg['es_predictions_index'], cfg['es_predictions_type'])
        _, result = get_ucdoc_prediction_count(
            ands, minus, bookings_map, day, es_client_predictions)

        # apply tbr on prediction values
        prediction_inventory = sum(result.values())
        tbr_ratio = amount * 1.0 / prediction_inventory
        result.update((x, int(y * tbr_ratio)) for x, y in result.items())

        # {'magazinelock,3,5G,g_x,2,pt,1004,icc': 788, 'minusonepage,1,5G,g_f,4,pt,1003,icc': 5017}
        resources = result

        # {'b2': 800, 'b3': 1000, 'b1': 500}
        demands = allocated

        # the sort of booking here is random
        allocation_map = hwm_generic_allocation(
            resources, resources.keys(), demands, demands.keys())

        return allocation_map

    _map_type = MapType(StringType(), IntegerType())
    df = df.withColumn('allocation_map', udf(helper, MapType(StringType(), _map_type))(
        df.ands, df.minus, df.amount, df.day, df.allocated))

    return df


def agg_allocation_maps(allocation_maps):
    result = {}
    for _map in allocation_maps:
        for key, value in _map.items():
            if key not in result:
                result[key] = 0
            result[key] += value

    return result


# if booking index has a doc with id 'lockBooking' then booking is locked
def lock_booking(es_client_booking, lock):
    if lock:
        es_client_booking.index(id=LOCK_BOOKING_ID, doc={})
    else:
        es_client_booking.delete(id=LOCK_BOOKING_ID)


def remove_booking_buckets(cfg, days):
    es_client_bb = ESClient(cfg['es_host'], cfg['es_port'],
                            cfg['es_bb_index'], cfg['es_bb_type'])
    time_ms = round(time.time()*1000)
    delete_bbs(days, time_ms, es_client_bb)


def save_booking_buckets_in_es(cfg, df):
    def helper(ands, minus, amount, day, allocated):
        es_client_bb = ESClient(cfg['es_host'], cfg['es_port'],
                                cfg['es_bb_index'], cfg['es_bb_type'])

        optimizer.dao.query_builder.index_bb(
            day, ands, minus, allocated, es_client_bb)
        return 1

    df = df.withColumn('pushed', udf(helper, IntegerType())(
        df.ands, df.minus, df.amount, df.day, df.allocated))

    return df

# Baohua Cao added this to cleanup code and enable unittest.
def generate_resources(cfg, df, bookings_map, days, bookings, hive_context):
    # booking json
    # {'days': ['2018-11-07'], 'bk_id': 'b7fc5b66-905f-4651-b370-0f1c475fcce1', 'adv_id': 'advid100', 'price': 0.0, 'amount': 0, 'query': {'r': None, 'g': ['g_m'], 'a': None, 't': None, 'si': None, 'm': None, 'aus': None, 'ais': None, 'pdas': None, 'exclude_pdas': None, 'apps': None, 'exclude_apps': None, 'dms': None, 'pm': 'NONE', 'dpc': None, 'ipl': None}, 'del': False}
    for day in days:
        added_bk_ids = []
        for booking in bookings:
            if not optimizer.util.valid_date(booking, day) or booking['bk_id'] in added_bk_ids:
                continue
            df = add_booking_to_bb_df(cfg,
                                      df, bookings_map, day, booking, added_bk_ids, hive_context)
            added_bk_ids.append(booking['bk_id'])
    return df

def run(cfg):

    global hive_context

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel('WARN')

    # ESClient requires host ip

    es_client_booking = ESClient(cfg['es_host'], cfg['es_port'],
                                 cfg['es_booking_index'], cfg['es_booking_type'])
    bookings = es_client_booking.search({})  # get at most 1000 results for now
    bookings = optimizer.util.filter_valid_bookings(bookings)
    # adjust dates in bookings
    bookings = optimizer.util.adjust_booking_dates(bookings)
    bookings_map = optimizer.util.get_bookings_map(bookings)

    df = hive_context.createDataFrame(sc.emptyRDD(), optimizer.util.get_common_pyspark_schema())
    today = cfg['today']  # YYYY-MM-DD
    days = optimizer.util.get_days_from_bookings(today, bookings)

    df = generate_resources(cfg, df, bookings_map, days, bookings, hive_context)
    # Row(day='2018-04-02', ands=['b1', 'b3', 'b2'], minus=[], allocated={}, amount=43562)
    print('defining resources')
    df.cache()
    print(df.take(1))

    # run the allocation
    df = hwm_allocation(df, bookings, days)

    # Row(day='2018-04-02', ands=['b1', 'b3', 'b2'], minus=[], amount=43562, allocated={'b2': 800, 'b3': 1000, 'b1': 500})
    print('bb-bookings allocation')
    df.cache()
    print(df.take(1))

    # lock bookings
    lock_booking(es_client_booking, True)

    # remove bbs
    remove_booking_buckets(cfg, days)

    # save new booking-buckets into es
    df = save_booking_buckets_in_es(cfg, df)
    print('bbs saved')
    df.cache()
    print(df.take(1))

    # unlock bookings
    lock_booking(es_client_booking, False)
    day = days[-1]
    tomorrow = optimizer.util.get_next_date(day)

    # use only tomorrow to create the allocation plan
    df = df.filter(df.day == tomorrow)

    # this method add the bbs ucdocs allocation_map with their values
    df = add_ucdoc_bb_allocation_map(cfg, df, bookings_map)

    # [Row(day='2018-04-02', ands=['b1', 'b3', 'b2'], minus=[], amount=43562, allocated={'b2': 800, 'b3': 1000, 'b1': 500}, allocation_map={'minusonepage,3,5G,g_x,2,pt,1002,icc': {'b2': 1, 'b3': 2, 'b1': 1}, 'magazinelock,2,3G,g_x,3,pt,1005,icc': {'b2': 56, 'b3': 70, 'b1': 35}, 'magazinelock,2,4G,g_x,3,pt,1005,icc': {'b2': 56, 'b3': 70, 'b1': 35}, 'minusonepage,3,5G,g_x,2,pt,1003,icc': {'b2': 6, 'b3': 8, 'b1': 4}, 'minusonepage,1,4G,g_x,2,pt,1003,icc': {'b2': 16, 'b3': 20, 'b1': 10}, 'minusonepage,2,4G,g_f,4,pt,1002,icc': {'b2': 12, 'b3': 15, 'b1': 8}, 'cloudFolder,2,5G,g_x,3,pt,1005,icc': {'b2': 57, 'b3': 72, 'b1': 36}, 'minusonepage,2,3G,g_x,3,pt,1002,icc': {'b2': 3, 'b3': 4, 'b1': 2}, 'minusonepage,1,3G,g_x,1,pt,1005,icc': {'b2': 27, 'b3': 33, 'b1': 17}, 'minusonepage,1,3G,g_x,4,pt,1004,icc': {'b2': 72, 'b3': 90, 'b1': 45}, 'magazinelock,2,5G,g_x,4,pt,1004,icc': {'b2': 32, 'b3': 40, 'b1': 20}, 'cloudFolder,2,3G,g_f,3,pt,1002,icc': {'b2': 16, 'b3': 20, 'b1': 10}, 'cloudFolder,3,5G,g_f,2,pt,1004,icc': {'b2': 27, 'b3': 34, 'b1': 17}})]
    print('ucdocs-bookings allocation')
    df.cache()
    print(df.take(1))

    # at this point we have a df which is a allocation of bookings to bbs
    df = df.select(df.day, explode(df.allocation_map))

    # Row(day='2018-04-02', key='magazinelock,3,5G,g_x,2,pt,1004,icc', value={'b2': 14, 'b3': 18, 'b1': 9})
    print('exploded')
    df.cache()
    print(df.take(1))

    # agg all the allocation maps for a ucdoc
    _map_type = MapType(StringType(), IntegerType())
    _audf = udf(agg_allocation_maps, _map_type)
    df = df.groupBy('key').agg(_audf(collect_list('value')).alias('allmap'))

    # [Row(key='cloudFolder,3,5G,g_f,2,pt,1004,icc', allmap={'b2': 27, 'b3': 34, 'b1': 17})]
    print('final aggregation')
    df.cache()
    print(df.take(1))

    # writing into hdfs
    filename = 'allmap-{}-{}'.format(
        optimizer.util.convert_date_remove_dash(day), str(int(time.time())))
    df.write.save(filename, format='json')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='optimizer')
    parser.add_argument('config_file')
    parser.add_argument('today', help='today %Y%m%d e.g. 20181230')
    args = parser.parse_args()
    # Load config file
    try:
        with open(args.config_file, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            cfg['today'] = args.today
            logger_operation.info(
                "Successfully open {}".format(args.config_file))
    except IOError as e:
        logger_operation.error(
            "Open config file unexpected error: I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        logger_operation.error("Unexpected error:{}".format(sys.exc_info()[0]))
        raise

    run(cfg)
