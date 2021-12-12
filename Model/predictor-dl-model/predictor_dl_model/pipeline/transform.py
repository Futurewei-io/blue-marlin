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

import math
import statistics

from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, explode, avg, stddev, size
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType, BooleanType


def _list_to_map(count_array):
    count_map = {}
    for item in count_array:
        key_value = item.split(':')
        count_map[key_value[0]] = key_value[1]
    return count_map


def add_count_map(df):
    # Convert count_array to count_map
    list_to_map_udf = udf(_list_to_map, MapType(
        StringType(), StringType(), False))
    df = df.withColumn('count_map', list_to_map_udf(df.count_array))
    return df

# This method replace zeros with nan and inject nans for non existing days


def calculate_time_series(df, col_name, day_list):
    def _helper(ts_list_map):
        ts_map = {}
        result = []
        for item_map in ts_list_map:
            for day, value in item_map.items():
                ts_map[day] = value
        for day in day_list:
            if day in ts_map:
                count = int(ts_map[day])
            else:
                count = float('nan')

            # replace zeros with nan
            if count == 0:
                count = float('nan')

            result.append(count)

        return result

    _udf = udf(_helper, ArrayType(IntegerType()))
    df = df.withColumn(col_name, _udf(df.ts_list_map))
    return df


def calculate_page_popularity(df):
    df = df.withColumn('page_popularity', udf(
        lambda x: statistics.median(x), FloatType())(df.ts_n))
    return df


# uph is uckey+price_cat+hour, in newer version the hour has been remove because count is aggregated on hour
def add_uph(df):
    df = df.withColumn('uph', udf(lambda x, y: ','.join(
        [x, str(y)]))(df.uckey, df.price_cat))
    return df


# This removes the ucdocs with more than limit 0 or nan
def clean_data(df, limit):
    def valid(ts):
        count = 0
        for i in ts:
            if i == 0 or math.isnan(i):
                count += 1
        if count > limit:
            return False
        return True
    df = df.filter(udf(valid, BooleanType())(df.ts))
    return df


def replace_ts_with_median(df, replace_zero):
    def _helper(ts):
        result = []
        num_list = [_ for _ in ts if not math.isnan(_) and _ != 0]
        median = 0
        if len(num_list) > 0:
            median = statistics.median(num_list)
        median = int(median)
        for i in ts:
            if math.isnan(i):
                result.append(median)
            if i == 0 and replace_zero:
                result.append(median)
            else:
                result.append(i)
        return result
    _udf = udf(_helper, ArrayType(IntegerType()))
    df = df.withColumn('ts_cured', _udf(df.ts))
    df = df.drop('ts')
    df = df.withColumnRenamed('ts_cured', 'ts')
    return df

def replace_nan_with_zero(df):
    def _helper(ts):
        result = []
        for i in ts:
            if i is None or math.isnan(i):
                result.append(0)
            else:
                result.append(i)
        return result
    _udf = udf(_helper, ArrayType(IntegerType()))
    df = df.withColumn('ts_zeroed', _udf(df.ts))
    df = df.drop('ts')
    df = df.withColumnRenamed('ts_zeroed', 'ts')
    return df

def add_feature_udf(i):
    def add_feature(uckey):
        features = uckey.split(',')
        if i < len(features):
            return features[i]
        else:
            return ''
    return udf(add_feature, StringType())


def add_ohe_feature(df, feature_name, feature_value_list):

    # This method is generalization of
    # df = df.withColumn('g_x', udf(lambda x: 1 if x == 'g_x' else 0, IntegerType())(df.g))
    def _helper(df, feature_name, feature_value):
        ohe_feature = feature_name + '_' + str(feature_value)
        df = df.withColumn(ohe_feature, udf(lambda x: 1 if x ==
                                            feature_value else 0, IntegerType())(df[feature_name]))
        return df

    for feature_value in feature_value_list:
        df = _helper(df, feature_name, feature_value)

    return df


def normalize_ohe_feature(df, ohe_feature):

    def _normalize(x, a, s):
        return (x - a)/(1.0 * s)

    _df = df.agg(avg(ohe_feature).alias('_avg'))
    avg_value = _df.take(1)[0]['_avg']

    _df = df.agg(stddev(ohe_feature).alias('_std'))
    std_value = _df.take(1)[0]['_std']

    if (std_value == 0):
        std_value = 1.0

    stats = [avg_value, std_value]

    df = df.withColumn(ohe_feature + '_n', udf(lambda x: _normalize(x,
                                                                    avg_value, std_value), FloatType())(df[ohe_feature]))

    return df, stats
