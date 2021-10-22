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

from ims_esclient import ESClient
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
import time
import pandas as pd

"""
This program based on the mode, get the list of dense or non-dense uckeys and get the total number of predicted value for each slot-id. 

spark-submit --master yarn  --num-executors 2 --executor-cores 5 --jars lib/elasticsearch-hadoop-6.8.0.jar test_dlpredictor_system_errors_11.py

Author: Faezeh
"""

mode = "SPARSE" #"DENSE"
si_list = [
    '06',
    '11',
    '05',
    '04',
    '03',
    '02',
    '01',
    'l03493p0r3',
    'x0ej5xhk60kjwq',
    'g7m2zuits8',
    'w3wx3nv9ow5i97',
    'a1nvkhk62q',
    'g9iv6p4sjy',
    'c4n08ku47t',
    'b6le0s4qo8',
    'd9jucwkpr3',
    'p7gsrebd4m',
    'a8syykhszz',
    'l2d4ec6csv',
    'j1430itab9wj3b',
    's4z85pd1h8',
    'z041bf6g4s',
    '71bcd2720e5011e79bc8fa163e05184e',
    'a47eavw7ex',
    '68bcd2720e5011e79bc8fa163e05184e',
    '66bcd2720e5011e79bc8fa163e05184e',
    '72bcd2720e5011e79bc8fa163e05184e',
    'f1iprgyl13',
    'q4jtehrqn2',
    'm1040xexan',
    'd971z9825e',
    'a290af82884e11e5bdec00163e291137',
    'w9fmyd5r0i',
    'x2fpfbm8rt',
    'e351de37263311e6af7500163e291137',
    'k4werqx13k',
    '5cd1c663263511e6af7500163e291137',
    '17dd6d8098bf11e5bdec00163e291137',
    'd4d7362e879511e5bdec00163e291137',
    '15e9ddce941b11e5bdec00163e291137']

# uc = "native,s4z85pd1h8,WIFI,g_m,5,CPC,40,80"

sc = SparkContext()
hive_context = HiveContext(sc)
sc.setLogLevel('WARN')

# si = "s4z85pd1h8"
###################################### read es and get the query!!!###############################
# read es
es_host = '10.213.37.41'
es_port = '9200'

# This is a correct index
es_index = 'dlpredictor_05062021_predictions'
es_type = 'doc'

# Load the prediction counts into a dictionary.
start_time = time.time()
es = ESClient(es_host, es_port, es_index, es_type)



def calc(s):
    command = """
        SELECT
        a.uckey
        from dlpm_03182021_tmp_ts as a 
        join 
        dlpm_03182021_tmp_distribution as b 
        on a.uckey = b.uckey where a.si ='{}' and b.ratio = 1
        """.format(s)
    df = hive_context.sql(command)
    dense_uckey = df.select('uckey').toPandas()

    command = """
        SELECT
        a.uckey
        from dlpm_03182021_tmp_ts as a 
        join 
        dlpm_03182021_tmp_distribution as b 
        on a.uckey = b.uckey where a.si = '{}' and b.ratio != 1
        """.format(s)
    df = hive_context.sql(command)
    sparse_uckey = df.select('uckey').toPandas()

    if mode == "DENSE":
        uckey_list = dense_uckey.uckey
    if mode == "SPARSE":
        uckey_list = sparse_uckey.uckey
    l = []
    for uc in uckey_list:
        body ={
                "size": 100,
                "query": {"bool": {"must": [

                    {"match": {
                        "_id": uc
                    }}

                ]}}

            }

        hits = es.search(body)


        es_records = {}
        uckeys = []
        predicted_days = set()
        for ucdoc in hits:
            uckey = ucdoc['uckey']
            uckeys.append(uckey)
            predictions = ucdoc['ucdoc']['predictions']
            # predictions = ["2020-06-01"]
            for day, hours in predictions.items():
                predicted_days.add(day)
                hour = -1
                h0 = h1 = h2 = h3 = 0
                for hour_doc in hours['hours']:
                    hour += 1
                    es_records[(uckey, day, hour, '0')] = hour_doc['h0']
                    es_records[(uckey, day, hour, '1')] = hour_doc['h1']
                    es_records[(uckey, day, hour, '2')] = hour_doc['h2']
                    es_records[(uckey, day, hour, '3')] = hour_doc['h3']
                    h0 += hour_doc['h0']
                    h1 += hour_doc['h1']
                    h2 += hour_doc['h2']
                    h3 += hour_doc['h3']
                    # print('h0: {} : {}  h1: {} : {}  h2: {} : {}  h3: {} : {}'.format(
                    #     hour_doc['h0'], h0, hour_doc['h1'], h1, hour_doc['h2'], h2, hour_doc['h3'], h3))

                es_records[(uckey, day, '0')] = h0
                es_records[(uckey, day, '1')] = h1
                es_records[(uckey, day, '2')] = h2
                es_records[(uckey, day, '3')] = h3
                es_records[(uckey, day)] = h0 + h1 + h2 + h3
                # print('daily: {}  {}  {}  {} : {}'.format(h0, h1, h2, h3, h0+h1+h2+h3))

                l.append([uckey,day, es_records[(uckey, day)]])


    data = pd.DataFrame(l, columns = ['uckey','day','agg'])
    agg_data = data.groupby(by = 'day').sum()
    return agg_data




for si in si_list:
    print(si,calc(s = si))
# agg_data.to_csv('/home/reza/faezeh/dense.csv')


