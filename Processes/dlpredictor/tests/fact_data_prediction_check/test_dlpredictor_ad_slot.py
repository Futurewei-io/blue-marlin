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


from client_rest_dl2 import predict

from pyspark import SparkContext
from pyspark.sql import HiveContext
import math
import time
import datetime

"""
Author: Eric Tsai

This file is to test the accuracy rate of the prediction for the uckeys of the 
given ad slot ids.  This program reads the predicted values from prediction 
server and actual counts from the ts table.

This script will create a CSV file with the uckey, the actual and predicted
daily impression counts for each day.  

The format of the CSV file is:
date,uckey,price_cat,slot_id,predicted,actual

Note: this script only retrieves the values for the dense uckeys in the ad slots.
"""


def get_model_stats(hive_context, model_stat_table):
    '''
    return a dict
    model_stats = {
        "model": {
            "name": "s32",
            "version": 1,
            "duration": 90,
            "train_window": 60,
            "predict_window": 10
        },
        "stats": {
            "g_g_m": [
                0.32095959595959594,
                0.4668649491714752
            ],
            "g_g_f": [
                0.3654040404040404,
                0.4815635452904544
            ],
            "g_g_x": [
                0.31363636363636366,
                0.46398999646418304
            ],
    '''
    command = """
            SELECT * FROM {}
            """.format(model_stat_table)
    df = hive_context.sql(command)
    rows = df.collect()
    if len(rows) != 1:
        raise Exception('Bad model stat table {} '.format(model_stat_table))
    model_info = rows[0]['model_info']
    model_stats = rows[0]['stats']
    result = {
        'model': model_info,
        'stats': model_stats
    }
    return result

def dl_daily_forecast(serving_url, model_stats, day_list, ucdoc_attribute_map, forward_offset=0):
    x, y = predict(serving_url=serving_url, model_stats=model_stats,
                   day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map, forward_offset=forward_offset)
    ts = x[0]
    days = y
    return ts, days

def normalize_ts(ts):
    ts_n = [math.log(i + 1) for i in ts]
    return ts_n

def predict_daily_uckey(sample, days, serving_url, model_stats, columns, forward_offset=0):

    def _denoise(ts):
        non_zero_ts = [_ for _ in ts if _ != 0]
        nonzero_p = 0.0
        if len(non_zero_ts) > 0:
            nonzero_p = 1.0 * sum(ts) / len(non_zero_ts)

        return [i if i > (nonzero_p / 10.0) else 0 for i in ts]

    def _helper(cols):
        day_list = days[:]
        ucdoc_attribute_map = {}
        for feature in columns:
            ucdoc_attribute_map[feature] = cols[feature]

        # determine ts_n and days
        model_input_ts = []

        # -----------------------------------------------------------------------------------------------
        '''
        The following code is in dlpredictor, here ts has a different format

        'ts': [0, 0, 0, 0, 0, 65, 47, 10, 52, 58, 27, 55, 23, 44, 38, 42, 90, 26, 95, 34, 25, 26, 18, 66, 31, 
        0, 38, 26, 30, 49, 35, 61, 0, 55, 23, 44, 35, 33, 22, 25, 28, 72, 25, 15, 29, 29, 9, 32, 18, 20, 70, 
        20, 4, 11, 15, 10, 8, 3, 0, 5, 3, 0, 23, 11, 44, 11, 11, 8, 3, 38, 3, 28, 16, 3, 4, 20, 5, 4, 45, 15, 9, 3, 60, 27, 15, 17, 5, 6, 0, 7, 12, 0],


        # ts = {u'2019-11-02': [u'1:862', u'3:49', u'2:1154'], u'2019-11-03': [u'1:596', u'3:67', u'2:1024']}
        ts = ucdoc_attribute_map['ts'][0]
        price_cat = ucdoc_attribute_map['price_cat']

        for day in day_list:
            imp = 0.0
            if day in ts:
                count_array = ts[day]
                for i in count_array:
                    parts = i.split(':')
                    if parts[0] == price_cat:
                        imp = float(parts[1])
                        break
            model_input_ts.append(imp)

    
        '''
        model_input_ts = ucdoc_attribute_map['ts']
        price_cat = ucdoc_attribute_map['price_cat']

        # --------------------------------------------------------------------------------------------------------

        # remove science 06/21/2021
        # model_input_ts = replace_with_median(model_input_ts)

        model_input_ts = _denoise(model_input_ts)

        ts_n = normalize_ts(model_input_ts)
        ucdoc_attribute_map['ts_n'] = ts_n

        # add page_ix
        page_ix = ucdoc_attribute_map['uckey'] + '-' + price_cat
        ucdoc_attribute_map['page_ix'] = page_ix

        rs_ts, rs_days = dl_daily_forecast(
            serving_url=serving_url, model_stats=model_stats, day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map, forward_offset=forward_offset)

        # respose = {'2019-11-02': 220.0, '2019-11-03': 305.0}

        response = {}
        for i, day in enumerate(rs_days):
            response[day] = rs_ts[i]
        return response

    return _helper(cols=sample)

def write_output(f, start_date, uckey, slot_id, price_cat, actual, predicted):
    # Print the data for each uckey.
    # date,uckey,price_cat,slot_id,predicted,actual
    for actual_count, predicted_count in zip(actual, predicted):
        f.write('{},"{}",{},{},{},{}\n'.format(start_date, uckey, price_cat, 
            slot_id, predicted_count, 0 if actual_count == None else actual_count))
        start_date = start_date + datetime.timedelta(days=1)


def run(cfg, hive_context):
    start_time = time.time()

    serving_url = cfg['serving_url']
    factdata_table = cfg['factdata_table']
    model_stat_table = cfg['model_stat_table']
    trainready_table = cfg['trainready_table']
    dist_table = cfg['dist_table']
    slot_ids = cfg['slot_ids']
    # cluster_uckeys = cfg['cluster_uckeys']
    # cluster_slot_ids = cfg['cluster_slot_ids']
    start_date = cfg['start_date']
    output_filepath = cfg['output_filepath']

    parsed_date = [ int(i) for i in start_date.split('-') ]
    start_date = datetime.date(parsed_date[0], parsed_date[1], parsed_date[2])

    model_stats = get_model_stats(hive_context, model_stat_table)
    predict_window = model_stats['model']['predict_window']
    print(model_stats)

    # Get the day list from the model stats.
    # day_list = [ (start_date + datetime.timedelta(days=i)).isoformat() for i in range(window_size) ]
    day_list = model_stats['model']['days']
    day_list.sort()
    print(day_list)

    with open(output_filepath, 'w') as f:
        # Print the header row.
        f.write('date,uckey,price_cat,slot_id,predicted,actual')
        f.write('\n')

        for slot_id in slot_ids:

            # Retrieve the sample from Hive so we can get the predicitons.
            df_trainready = hive_context.sql(
                'SELECT * FROM {} WHERE uckey LIKE \'%{}%\''.format(trainready_table, slot_id))
            df_dist = hive_context.sql(
                'SELECT * FROM {} WHERE ratio=1 AND uckey LIKE \'%{}%\''.format(dist_table, slot_id))
            df = df_trainready.join(
                df_dist, on=['uckey', 'price_cat'], how='inner')
            df_ts = hive_context.sql('SELECT uckey, price_cat, ts_ver, si FROM {}'.format(factdata_table))
            df = df.join(df_ts, on=['uckey', 'price_cat'], how='inner')
            columns = df.columns
            samples = df.collect()

            if len(samples) == 0:
                print('No samples for {}'.format(slot_id))
                continue

            print('Sample found for {}'.format(slot_id))
            for i, samp in enumerate(samples):
                print('Processing sample {} of {} for slot id {}'.format(i, len(samples), slot_id))

                sample = {}
                for feature in columns:
                    sample[feature] = samp[feature]
                
                uckey = sample['uckey']
                price_cat = sample['price_cat']
                whole_ts = sample['ts'][:]
                # expected = whole_ts[-predict_window:]
                sample['ts'] = whole_ts[:-predict_window]
                # input_ts = sample['ts']
                actual = sample['ts_ver'][1:]

                # Get the predicted count.
                response = predict_daily_uckey(
                    sample=sample, days=day_list, serving_url=serving_url, model_stats=model_stats, columns=columns)
                predicted = [response[i] for i in sorted(response)]
                # Overlap between predicted and actual misses the last entry of predicted.
                predicted = predicted[:-1]

                print('{} : {}\nactual:    {}\npredicted: {}'.format(uckey, price_cat, actual, predicted))
                write_output(f, start_date, uckey, slot_id, price_cat, actual, predicted)

    print('Runtime of the program is:', str(datetime.timedelta(seconds=time.time() - start_time)))



if __name__ == '__main__':

    cfg = {
        'log_level': 'warn',
        'factdata_table':   'dlpm_111021_no_residency_no_mapping_tmp_ts',
        'trainready_table': 'dlpm_111021_no_residency_no_mapping_trainready',
        'dist_table':       'dlpm_111021_no_residency_no_mapping_tmp_distribution',
        'model_stat_table': 'dlpm_111021_no_residency_no_mapping_model_stat',
        'serving_url': 'http://10.193.217.126:8506/v1/models/dl_no_mapped_ipl:predict',
        # 'sample_ratio': 1,
        # 'max_calls': 1000,
        # 'yesterday': 'WILL BE SET IN PROGRAM',
        'start_date': '2021-07-22',
        # 'window_size': 9,
        'output_filepath': 'dlpredictor_ad_slot.csv',
        'slot_ids': [ 'b6le0s4qo8', '17dd6d8098bf11e5bdec00163e291137' ],
        # 'cluster_uckeys': [ 1 ],
        # 'cluster_slot_ids': [ '17dd6d8098bf11e5bdec00163e291137' ],
    }

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    run(cfg=cfg, hive_context=hive_context)
