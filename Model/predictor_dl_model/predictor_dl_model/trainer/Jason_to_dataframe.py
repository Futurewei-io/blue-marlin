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

from pandasticsearch import DataFrame
import pandas as pd
import numpy as np
import os
from pandas import isnull


def get_data():
    def el_to_df():

        df = DataFrame.from_es(url='http://10.193.217.111:9200', index='predictions_test_generated_11142019')
        table = df.limit(212).to_pandas()

        return table

    ## set the usdoc keys as an index
    df = el_to_df().set_index(['ucdoc.uckey'])

    ##store it in dummy dataframe
    df_temp = df

    # get rid of columns that are not time
    df = df[df.columns[pd.Series(df.columns).str.startswith('ucdoc.days')]]

    df_temp = df_temp[['ucdoc.a', 'ucdoc.si', 'ucdoc.r', 'ucdoc.t', 'ucdoc.g']]

    # df = df.applymap(lambda x: {} if df.isnull(x) else x)

    df_value = np.zeros(df.shape, dtype=int)

    def total_hours(df):
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                if type(df.iloc[i, j]) == list:
                    df_value[i, j] = sum([x['total'] for x in df.iloc[i, j]])
                else:
                    df_value[i, j] = 0
        return df_value

    df_value = total_hours(df)

    df = pd.DataFrame(data=df_value, index=df.index, columns=df.columns)
    df.rename(columns=lambda x: str(x)[-10:], inplace=True)
    df = df.reindex(sorted(df.columns), axis=1)

    return df,df_temp


df, df_temp = get_data()


def Holiday(df):

    holidays = ["2018-01-01", "2018-01-15", "2018-02-19", "2018-05-28", "2018-03-09"]

    valArray = []
    for column in df.columns.values:
        if column in holidays:
            val = 1
        else:
            val = 0
        valArray.append(val)

    holiday_df = pd.DataFrame([valArray], columns=df.columns.values)
    return holiday_df

df_holiday = Holiday(df)

path = os.path.join('data', 'all.pkl')
path2 = os.path.join('data', 'all2.pkl')
path3 = os.path.join('data', 'holiday.pkl')
df.to_pickle(path)
df_temp.to_pickle(path2)
df_holiday.to_pickle(path3)
