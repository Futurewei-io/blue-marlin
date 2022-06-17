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

import yaml
import argparse
from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql import HiveContext
from util import resolve_placeholder
import pandas as pd
import numpy as np

'''
spark-submit --master yarn --py-files pipeline/transform.py --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G pipeline/main_outlier_2.py config.yml

THIS MODULE HAS NOT BEEN TESTED.

'''


def median_absolute_deviation(x):
    """
    Returns the median absolute deviation from the window's median
    :param x: Values in the window
    :return: MAD
    """
    return np.median(np.abs(x - np.median(x)))


def hampel(ts, window_size=5, n=3, imputation=False):
    """
    Median absolute deviation (MAD) outlier in Time Series
    :param ts: a pandas Series object representing the timeseries
    :param window_size: total window size will be computed as 2*window_size + 1
    :param n: threshold, default is 3 (Pearson's rule)
    :param imputation: If set to False, then the algorithm will be used for outlier detection.
        If set to True, then the algorithm will also imput the outliers with the rolling median.
    :return: Returns the outlier indices if imputation=False and the corrected timeseries if imputation=True
    """

    if type(ts) != pd.Series:
        raise ValueError("Timeserie object must be of tyme pandas.Series.")

    if type(window_size) != int:
        raise ValueError("Window size must be of type integer.")
    else:
        if window_size <= 0:
            raise ValueError("Window size must be more than 0.")

    if type(n) != int:
        raise ValueError("Window size must be of type integer.")
    else:
        if n < 0:
            raise ValueError("Window size must be equal or more than 0.")

    # Copy the Series object. This will be the cleaned timeserie
    ts_cleaned = ts.copy()

    # Constant scale factor, which depends on the distribution
    # In this case, we assume normal distribution
    k = 1.4826

    rolling_ts = ts_cleaned.rolling(window_size*2, center=True)
    rolling_median = rolling_ts.median().fillna(
        method='bfill').fillna(method='ffill')
    rolling_sigma = k * \
        (rolling_ts.apply(median_absolute_deviation).fillna(
            method='bfill').fillna(method='ffill'))

    outlier_indices = list(
        np.array(np.where(np.abs(ts_cleaned - rolling_median) >= (n * rolling_sigma))).flatten())

    if imputation:
        ts_cleaned[outlier_indices] = rolling_median[outlier_indices]
        return ts_cleaned

    return outlier_indices


def write_to_table(df, table_name, mode='overwrite'):
    df.write.option("header", "true").option(
        "encoding", "UTF-8").mode(mode).format('hive').saveAsTable(table_name)


def run_outlier(df, window_size, n):

    def fix_format(ts):
        return [int(i) if i is not None else int(0) for i in ts]

    def _hampel(ts):
        ts = fix_format(ts)
        ts = pd.Series(list(ts))
        _ts = hampel(ts, window_size=window_size, n=n, imputation=True)
        ts = fix_format(_ts)
        return ts

    def _hampel_detect(ts):
        ts = fix_format(ts)
        ts = pd.Series(list(ts))
        outlier_indices = hampel(ts, window_size=window_size, n=n)
        return len(outlier_indices)

    df = df.withColumn('outlier_len', udf(
        _hampel_detect, IntegerType())(df['ts']))
    df = df.withColumn('ts', udf(_hampel, ArrayType(IntegerType()))(df['ts']))

    return df


def run(hive_context, input_table_name, outlier_table, outlier_window_size, outlier_n):

    # Read factdata table
    command = """
    SELECT * FROM {}
    """.format(input_table_name)

    # DataFrame[ts: array<int>]
    df = hive_context.sql(command)
    df = run_outlier(df=df, window_size=outlier_window_size, n=outlier_n)

    write_to_table(df, outlier_table, mode='overwrite')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg = cfg['pipeline']

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    input_table_name = cfg['outlier']['input_table_name']
    outlier_table = cfg['outlier']['output_table_name']
    outlier_window_size = cfg['outlier']['window_size']
    outlier_n = cfg['outlier']['n']

    run(hive_context=hive_context,
        input_table_name=input_table_name,
        outlier_table=outlier_table, outlier_window_size=outlier_window_size, outlier_n=outlier_n)

    sc.stop()
