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

import datetime
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType


def valid_date(booking, today):
    if today in booking['days']:
        return True
    return False


def get_days_from_bookings(today, bookings):
    days = set()
    for booking in bookings:
        days = days.union(booking['days'])
    days = [day for day in days if day >= today]
    days.sort()
    return days


def get_bookings_map(bookings):
    result = {}
    for booking in bookings:
        result[booking['bk_id']] = booking
    return result

# converts YYYYMMDD to YYYY-MM-DD


def convert_date_add_dash(date):
    if len(date) != 8:
        raise ValueError('Wrong date format')
    return '-'.join([date[0:4], date[4:6], date[6:]])

# converts YYYY-MM-DD to YYYYMMDD


def convert_date_remove_dash(date):
    if len(date) != 10:
        raise ValueError('Wrong date format')
    return date[0:4]+date[5:7]+date[8:]


def adjust_booking_dates(bookings):
    for booking in bookings:
        new_dates = []
        for day in booking['days']:
            day_with_dash = convert_date_add_dash(day)
            new_dates.append(day_with_dash)
        booking['days'] = new_dates
    return bookings


def get_date(date_y_m_d):
    return datetime.datetime.strptime(date_y_m_d, '%Y-%m-%d')


def get_next_date(date_y_m_d):
    date = get_date(date_y_m_d)
    date += datetime.timedelta(days=1)
    result = date.strftime('%Y-%m-%d')
    return result


def filter_valid_bookings(bookings):
    result = []
    for doc in bookings:
        valid = True
        for attr in ['days', 'query', 'bk_id', 'amount']:
            if attr not in doc:
                valid = False
                break
        if 'del' in doc and str(doc['del']).lower == 'false':
            valid = False
        if valid:
            result.append(doc)
    return result


def get_common_pyspark_schema():
    schema = StructType([StructField('day', StringType(), True),
                        StructField('ands', ArrayType(StringType()), True),
                        StructField('minus', ArrayType(StringType()), True),
                        StructField('allocated', MapType(
                            StringType(), IntegerType()), True),
                        StructField('amount', IntegerType(), True)])
    return(schema)