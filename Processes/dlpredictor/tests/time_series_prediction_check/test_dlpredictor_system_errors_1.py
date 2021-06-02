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


from imscommon.es.ims_esclient import ESClient
from pyspark import SparkContext
from pyspark.sql import HiveContext
import hashlib
import math
import time
import datetime
import os
import datetime

"""
Author: Eric Tsai

This script pulls the predicted and observed daily count data for the given
set of uckeys.  The list of uckeys to load the data for is taken from a text file.
The predicted traffic comes from Elasticsearch.  The observed traffic comes from
the time series table.

This script will create a CSV file with the uckey, the actual and predicted
daily impression count, and the total actual and predicted counts for the 
overlapping days, plus all the observed and predicted daily traffic.
"""

# Elasticsearch server info.
es_host = '10.213.37.41'
es_port = '9200'
# es_index = 'dlpredictor_05062021_predictions'
es_index = 'dlpredictor_05182021_1500_predictions'
es_type = 'doc'

# The time series table to load the observed data from./
# ts_table = 'dlpm_03182021_tmp_ts'
ts_table = 'dlpm_05182021_1500_tmp_ts'

# The path to the file to load the uckey names.
dense_uckey_file = 'dense_uckeys.txt'
dense_uckey_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), dense_uckey_file)
sparse_uckey_file = 'sparse_uckeys.txt'
sparse_uckey_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), sparse_uckey_file)

# Name of file that the script will write containing the uckeys, actual and
# predicted daily counts, overlapping day totals, and the predicted and actual daily counts.
dense_output_filepath = 'dense_dlpredictor.csv'
sparse_output_filepath = 'sparse_dlpredictor.csv'

# The number of uckeys to load per batch.
step_size = 1000  # 1000 is the approximate maximum before Elasticsearch throws errors.

# The first day in the time series data.
start_date = datetime.date(2020, 6, 1) - datetime.timedelta(days = 90)

def load_uckeys(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()
        return [ line.strip() for line in lines ]

def print_es_query(query):
    print_es_query_element(query, 0)

def print_es_query_element(element, indent):
    if element is dict:
        print('{')
        for key, value in element.items():
            indent += 1
            print('\"{}\": '.format(key))
            print(value)
        print('}')
    elif element is list:
        print('[')
        first = True
        for key, value in element:
            indent += 1
            if not first:
                print(',')
            print('\"{}\": '.format(key))
            print_es_query_element(value, indent)
        print(']')
    elif element is str:
        print('\"{}\"'.format(element))
    else:
        print(element)

def segment(end, multiple):
    result = []
    round_up = int(math.ceil(float(end) / multiple) * multiple)
    for i in range(multiple, round_up + 1, multiple):
        if i <= end:
            result.append(i)
        else:
            result.append(end)
    return result

def get_predictions(es, uckeys):
    es_records = {}
    found_uckeys = []
    predicted_days = set()

    # Execute the Elasticsearch query.
    batch_total = 0
    terms = [ { "term": { "ucdoc.uckey.keyword" : uckey }} for uckey in uckeys ]
    query = { "size": len(uckeys), "query": {"bool": {"should": terms}} }
    # print_es_query(query)
    hits = es.search(query)

    # Parse the Elasticsearch search results.
    for ucdoc in hits:
        uckey = ucdoc['uckey']
        # print(uckey)
        found_uckeys.append(uckey)
        predictions = ucdoc['ucdoc']['predictions']
        for day, hours in predictions.items():
            predicted_days.add(day)
            # hour = -1
            h0 = h1 = h2 = h3 = 0
            for hour_doc in hours['hours']:
                h0 += hour_doc['h0']
                h1 += hour_doc['h1']
                h2 += hour_doc['h2']
                h3 += hour_doc['h3']

            es_records[(uckey, day, 0)] = h0
            es_records[(uckey, day, 1)] = h1
            es_records[(uckey, day, 2)] = h2
            es_records[(uckey, day, 3)] = h3

            total = h0 + h1 + h2 + h3
            es_records[(uckey, day)] = total
            batch_total += total
            # print('daily: {}  {}  {}  {} : {}'.format(h0, h1, h2, h3, h0+h1+h2+h3))

    return es_records, found_uckeys, predicted_days, batch_total

def generate_command(table, uckeys):
    formatted_uckeys = [ '\"{}\"'.format(uckey) for uckey in uckeys]
    return 'select * from {} where uckey in ({})'.format(table, ','.join(formatted_uckeys))

def get_observed(hive_context, uckeys):
    observed_counts = {}
    observed_days = set()

    command = generate_command(ts_table, uckeys)
    # print(command)
    df = hive_context.sql(command)

    batch_total = 0
    for row in df.collect():
        uckey = row['uckey']
        price_cat = int(row['price_cat'])
        ts = row['ts']

        # Store each day in the dictionary separately.
        day = start_date
        day_delta = datetime.timedelta(days = 1)
        # print(ts)
        for count in ts:
            if count == None:
                count = 0
            # print('{}  {}  {}  {} : {}'.format(uckey, day, hour, price_cat, i))
            day_str = day.isoformat()
            observed_counts[(uckey, day_str, price_cat)] = count
            batch_total += count
            observed_days.add(day_str)
            day = day + day_delta
    return observed_counts, observed_days, batch_total

def extract_uckey_traffic(uckey_path, output_filepath, start_time, es, hive_context):
    # Load the uckeys to verify.
    dense_uckeys = load_uckeys(uckey_path)

    index = 0
    first = True

    predicted_total = 0
    observed_total = 0

    with open(output_filepath, 'w') as f:
        for i in segment(len(dense_uckeys), step_size):
            uckeys = dense_uckeys[index : i]

            # Load the prediction counts for this batch of uckeys.
            predicted_counts, found_uckeys, predicted_days, batch_total = get_predictions(es, uckeys)
            predicted_total += batch_total

            print('found: {} of {} predictions'.format(len(found_uckeys), len(uckeys)))
            print('index: {} of {} uckeys'.format(index, len(dense_uckeys)))
            print('predicted total: {}'.format(predicted_total))

            # Load the observed counts for this batch of uckeys.
            observed_counts, observed_days, batch_total = get_observed(hive_context, uckeys)
            observed_total += batch_total
            print('observed total: {}'.format(observed_total))

            # Print the header row.
            observed_days_sorted = sorted(observed_days)
            predicted_days_sorted = sorted(predicted_days)
            if first:
                f.write('uckey,slot_type,slot_id,network,gender,age,price_type,region,ipl,price_cat,actual_total,predicted_total,mape')
                for day in observed_days_sorted:
                    f.write(',actual_{}'.format(day))
                for day in predicted_days_sorted:
                    f.write(',predicted_{}'.format(day))
                f.write('\n')
                first = False

            # Compare the predicted and observed counts.
            for uckey in uckeys:
                for price_cat in range(4):
                    # Calculate the total predicted and actual counts for the days
                    # where the predictions and the observed data overlap.
                    total_count = 0
                    total_pcount = 0
                    for day in sorted(predicted_days.intersection(observed_days)): # Filter for overlapping days
                        # print(day)
                        key = (uckey, day, price_cat)
                        pcount = 0
                        if key in predicted_counts:
                            pcount = predicted_counts[key]
                        total_pcount += pcount
                        # print('pcount: {}'.format(pcount))
                        # print('total_pcount: {}'.format(total_pcount))
                        count = 0
                        if key in observed_counts:
                            # print('found observed')
                            count = observed_counts[key]
                            # print('{:.3f} - {} = {:.3f}'.format(pcount, count, pcount - count))
                        total_count += count
                        # print('   count: {}'.format(count))
                        # print('   total_count: {}'.format(total_count))

                    # Print the data for this uckey if there is observed or predicted traffic.
                    # Since we are cycling through every uckey/price category combination, there
                    # will be iterations where there is neither observed nor predicted traffic 
                    # on the overlapping days.
                    if total_count != 0 or total_pcount != 0:
                        f.write('"{}",{},{},{},{},{}'.format(uckey, uckey, price_cat, total_count, total_pcount, 
                            abs(total_pcount - total_count)/total_count if total_count != 0 else 0))

                        # Print the observed data.
                        for day in observed_days_sorted:
                            key = (uckey, day, price_cat)
                            if key in observed_counts:
                                count = observed_counts[key]
                                if count != None:
                                    f.write(',{}'.format(count))
                                else:
                                    f.write(',0')
                            else:
                                f.write(',0')

                        # Print the predicted data.
                        for day in predicted_days_sorted:
                            key = (uckey, day, price_cat)
                            if key in predicted_counts:
                                f.write(',{}'.format(predicted_counts[key]))
                            else:
                                f.write(',0')
                        f.write('\n')


            print('total elapsed time: {:.1f}s'.format(time.time() - start_time))
            index += step_size
    return predicted_total, observed_total

if __name__ == '__main__':
    es = ESClient(es_host, es_port, es_index, es_type)
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel('WARN')

    start_time = time.time()

    dense_predicted_total, dense_observed_total = extract_uckey_traffic(dense_uckey_path, dense_output_filepath, start_time, es, hive_context)
    sparse_predicted_total, sparse_observed_total = extract_uckey_traffic(sparse_uckey_path, sparse_output_filepath, start_time, es, hive_context)

    print('Dense predicted total: {}'.format(dense_predicted_total))
    print('Dense observed total:  {}'.format(dense_observed_total))
    print('Sparse predicted total: {}'.format(sparse_predicted_total))
    print('Sparse predicted total: {}'.format(sparse_observed_total))

