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
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType
import hashlib
import math
import time
import datetime

"""
Author: Eric Tsai

This file is to test the accuracy rate of the prediction.
The assumption is that es prediction and factdata has intersections on days.
This program reads the predicted values from ES and calcualte the MAPE Error.

This script will read in a configurable number of ucdocs from ES and compare the 
predictions for those uckeys with the observed uckeys.

This script will create a CSV file with the uckey, the actual and predicted
daily impression count, and the total actual and predicted counts for the 
overlapping days.
"""

# read es
es_host = '10.213.37.41'
es_port = '9200'

# This is a correct index
es_index = 'dlpredictor_05062021_predictions'
es_type = 'doc'

# The number of uckeys to check the predictions of.
number_of_uckeys_to_read = 5000 # 5000

# The name of the factdata_table from which to read the actual impression count. 
# The factdata_table should be the same that is used for dlpredictor.
factdata_table = 'dlpm_03182021_tmp_area_map'

# Name of file that the script will write containing the uckeys, actual and
# predicted daily counts, overlapping day totals.
output_filepath = 'count_dlpredictor.csv'

# uckeys = [ 
#     "native,a47eavw7ex,4G,g_m,2,CPM,35,", "native,a47eavw7ex,WIFI,g_m,3,CPM,5,5", "native,a47eavw7ex,WIFI,g_m,6,CPM,46,46", 
#     "native,l03493p0r3,4G,g_m,2,CPC,22,22", "splash,w9fmyd5r0i,4G,,,CPM,,79", "splash,w9fmyd5r0i,4G,,,CPM,,78", 
#     "splash,w9fmyd5r0i,4G,,,CPM,,77", "native,a47eavw7ex,4G,g_m,4,CPM,72,60", "splash,w9fmyd5r0i,4G,,,CPM,,75", 
#     "splash,w9fmyd5r0i,4G,,,CPM,,74", "splash,w9fmyd5r0i,4G,,,CPM,,73", "splash,w9fmyd5r0i,4G,,,CPM,,72", 
#     "splash,w9fmyd5r0i,4G,,,CPM,,71", "splash,w9fmyd5r0i,4G,,,CPM,,70", "native,x0ej5xhk60kjwq,WIFI,g_m,1,CPC,40,", 
#     "native,w3wx3nv9ow5i97,WIFI,g_f,1,CPC,81,81", "native,a8syykhszz,WIFI,g_f,5,CPC,76,76", "native,66bcd2720e5011e79bc8fa163e05184e,4G,g_m,3,CPC,30,30", 
#     "native,a47eavw7ex,WIFI,g_f,2,CPM,58,58", "native,x0ej5xhk60kjwq,WIFI,g_m,5,CPC,,35", "native,x0ej5xhk60kjwq,WIFI,g_m,3,CPC,52,52", 
#     "native,x0ej5xhk60kjwq,WIFI,g_m,3,CPC,78,", "native,b6le0s4qo8,WIFI,g_f,2,CPC,70,5", "native,w3wx3nv9ow5i97,WIFI,g_m,5,CPC,72,78", 
#     "roll,f1iprgyl13,WIFI,g_m,5,CPM,79,79", "native,w3wx3nv9ow5i97,WIFI,g_m,5,CPC,72,76", "native,w3wx3nv9ow5i97,WIFI,g_m,5,CPC,72,74", 
#     "magazinelock,04,WIFI,g_m,2,CPM,76,", "native,w3wx3nv9ow5i97,WIFI,g_m,5,CPC,72,72", "native,w3wx3nv9ow5i97,WIFI,g_m,5,CPC,72,71", 
#     "native,x0ej5xhk60kjwq,WIFI,g_f,5,CPM,40,", "native,g7m2zuits8,WIFI,g_m,3,CPM,72,76", "native,x0ej5xhk60kjwq,WIFI,g_m,6,CPC,74,74", 
#     "native,x0ej5xhk60kjwq,WIFI,g_m,6,CPC,74,73", "native,g7m2zuits8,WIFI,g_m,3,CPM,72,71", "native,a47eavw7ex,WIFI,g_m,4,CPM,71,68", 
#     "native,a47eavw7ex,WIFI,g_m,4,CPM,71,69", "native,a47eavw7ex,WIFI,g_m,4,CPM,71,67", "native,a47eavw7ex,WIFI,g_m,4,CPM,71,65", 
#     "native,b6le0s4qo8,4G,g_m,3,CPC,77,28", "native,a47eavw7ex,WIFI,g_m,4,CPM,71,63", "roll,f1iprgyl13,4G,g_m,4,CPM,80,47", 
#     "splash,a290af82884e11e5bdec00163e291137,4G,g_m,5,CPM,24,24", "native,d9jucwkpr3,WIFI,g_m,4,CPC,77,77", "roll,f1iprgyl13,4G,g_m,4,CPM,80,40", 
#     "native,a47eavw7ex,WIFI,g_f,3,CPC,71,80", "native,a47eavw7ex,WIFI,g_f,3,CPC,71,81", "native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,6,CPC,11,11", 
#     "native,a47eavw7ex,4G,g_f,2,CPM,67,45", "roll,q4jtehrqn2,WIFI,g_m,6,CPM,21,21", "native,g7m2zuits8,4G,g_m,2,CPM,18,18", 
#     "native,a47eavw7ex,4G,g_f,4,CPC,38,64", "native,66bcd2720e5011e79bc8fa163e05184e,4G,g_m,3,CPM,72,72", "native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_f,5,CPC,13,13", 
#     "native,g7m2zuits8,WIFI,g_m,5,CPM,,76", "native,a47eavw7ex,4G,g_m,6,CPM,24,24", "native,b6le0s4qo8,WIFI,g_m,5,CPC,49,", 
#     "native,x0ej5xhk60kjwq,4G,g_m,4,CPM,32,32", "native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,6,CPC,75,78", "native,a47eavw7ex,4G,g_m,4,CPC,77,28", 
#     "native,x0ej5xhk60kjwq,4G,g_m,4,CPM,1,80", "native,x0ej5xhk60kjwq,3G,,,CPC,,29", "roll,q4jtehrqn2,4G,g_m,4,CPM,78,51", 
#     "native,a47eavw7ex,4G,g_m,4,CPC,77,24", "native,a47eavw7ex,4G,g_f,6,CPC,80,1", "native,a47eavw7ex,4G,g_m,4,CPC,77,27", 
#     "native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,6,CPC,75,75", "native,a47eavw7ex,4G,g_m,4,CPC,77,22", "roll,f1iprgyl13,4G,g_m,2,CPM,74,74", 
#     "roll,f1iprgyl13,WIFI,g_f,6,CPM,13,13", "native,z041bf6g4s,WIFI,g_m,5,CPC,53,53", "splash,e351de37263311e6af7500163e291137,WIFI,g_f,3,CPM,71,71", 
#     "native,x0ej5xhk60kjwq,WIFI,g_f,5,CPM,11,11", "native,a47eavw7ex,WIFI,g_m,3,CPC,26,26", "native,a47eavw7ex,WIFI,g_m,4,CPC,8,63", 
#     "native,a47eavw7ex,WIFI,g_m,1,CPC,54,54", "native,l03493p0r3,WIFI,g_m,3,CPC,46,72", "native,a47eavw7ex,4G,g_m,4,CPM,72,9", 
#     "native,x0ej5xhk60kjwq,WIFI,g_f,1,CPM,80,80", "native,a47eavw7ex,WIFI,g_m,6,CPC,46,46", "magazinelock,04,WIFI,g_m,5,CPM,78,46", 
#     "native,b6le0s4qo8,4G,g_m,6,CPC,71,", "roll,q4jtehrqn2,WIFI,g_m,6,CPM,63,63", "magazinelock,04,WIFI,g_m,6,CPM,9,71", 
#     "roll,q4jtehrqn2,WIFI,g_m,5,CPM,37,", "native,a47eavw7ex,4G,g_m,4,CPM,72,65", "native,x0ej5xhk60kjwq,WIFI,g_f,2,CPC,60,60", 
#     "native,66bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,10,10", "native,x0ej5xhk60kjwq,WIFI,g_f,2,CPC,26,22", "native,x0ej5xhk60kjwq,4G,g_m,5,CPC,39,", 
#     "roll,f1iprgyl13,WIFI,g_m,6,CPM,69,69", "native,a47eavw7ex,4G,g_m,6,CPC,9,9", "native,z041bf6g4s,4G,g_m,4,CPC,37,", 
#     ]


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
        for key, value in element.items():
            indent += 1
            if not first:
                print(',')
            print('\"{}\": '.format(key))
            print_es_query_element(value, indent)
        print(']')
    else:
        print(element)

def get_predictions(uckeys):
    # Execute the Elasticsearch query.
    es = ESClient(es_host, es_port, es_index, es_type)
    if uckeys:
        terms = [ { "term": { "ucdoc.uckey.keyword" : uckey }} for uckey in uckeys ]
        query = { "size": 1000, "query": {"bool": {"should": terms}} }
        print_es_query(query)
        hits = es.search(query)
    else:
        hits = es.search({"size": number_of_uckeys_to_read})

    # Parse the Elasticsearch search results.
    es_records = {}
    found_uckeys = []
    predicted_days = set()
    for ucdoc in hits:
        uckey = ucdoc['uckey']
        print(uckey)
        found_uckeys.append(uckey)
        predictions = ucdoc['ucdoc']['predictions']
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
    print('found: {}  requested: {}'.format(len(found_uckeys), len(uckeys)))
    return es_records, found_uckeys, predicted_days

def get_bucket_id(s, num_buckets = 10):
    hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
    return int(hex_value, 16) % num_buckets

def sort_uckeys_by_bucket(uckeys):
    bucket_uckeys = {}
    for uckey in uckeys:
        bucket = get_bucket_id(uckey)
        if not bucket in bucket_uckeys:
            bucket_uckeys[bucket] = []
        bucket_uckeys[bucket].append(uckey)
    return bucket_uckeys

def generate_command(table, uckeys, bucket_id):
    # return 'select * from {} where uckey = \"{}\"'.format(table, uckeys[0])  # TODO: Remove this line
    conditions = [ 'uckey = \"{}\"'.format(uckey) for uckey in uckeys ]
    return 'select * from {} where bucket_id = {} AND ({})'.format(
        table, bucket_id, ' OR '.join(conditions))

def parse_count (count_array):
    output = dict()
    for i in count_array:
        (price_cat, count) = i.split(':')
        output[price_cat] = int(count)
    return output

# def sum_count_array(count_array):
#     '''
#     ["2:28","1:15"]
#     '''
#     count = 0
#     for item in count_array:
#         _, value = item.split(':')
#         count += int(value)
# 
#     return count


if __name__ == '__main__':

    # Load the prediction counts into a dictionary.
    start_time = time.time()
    es_records, uckeys, predicted_days = get_predictions(uckeys)


    # Sort uckeys by bucket.
    # uckeys = ['native,l03493p0r3,WIFI,g_m,4,CPC,18,']
    # uckeys = ['native,g7m2zuits8,WIFI,g_m,3,CPM,15,79']  # TODO: Remove this line
    bucket_uckeys = sort_uckeys_by_bucket(uckeys)

    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel('WARN')

    all_uckeys = []
    all_errors = []
    all_found  = []
    all_agg_errors = []
    all_counts = []
    all_pcounts = []
    all_times = [time.time()]
    all_elapsed = []
    all_days = set()
    # for uckey in uckeys:
    day_count = dict()
    for bucket_id in bucket_uckeys.keys():
        uckeys = bucket_uckeys[bucket_id]
        print('1: Create query')
        command = generate_command(factdata_table, uckeys, bucket_id)
        print(command)
        df = hive_context.sql(command)
        df_collect = df.collect()

        # df = df.withColumn('count', udf(sum_count_array, IntegerType())(df.count_array))
        # l = df.agg({'count': 'sum'}).take(1)
        # if (len(l) > 0):
        #     total_count += l[0]["sum(count)"]


        # Collect the observed counts across the days and price categories.
        print('2: Processing the SQL query')
        hour_count = dict()
        for row in df_collect:
            uckey = row['uckey']
            hour = row['hour']
            day = row['day']
            # print(day)
            all_days.add(day)
            count = parse_count(row['count_array'])
            # Split out the price categories from count.
            for price_cat, i in count.items():
                # print('{}  {}  {}  {} : {}'.format(uckey, day, hour, price_cat, i))
                hour_count[(uckey, day, hour, str(price_cat))] = i

        print('3: Aggregate daily rate')
        for uckey in uckeys:
            # Calculate the impression count total across days and price categories
            for day in sorted(all_days):
                is_found = False
                count = 0
                for price_cat in range(4):
                    for hour in range(24):
                        key = (uckey, day, hour, str(price_cat))
                        if key in hour_count:
                            is_found = True
                            # print('{}  {}  {}  {} : {} : {}'.format(uckey, day, hour, price_cat, hour_count[key], count))
                            count += hour_count[key]
                # Only add the day if it's in the fact table.
                # if is_found:
                day_count[(uckey, day)] = count
                print('\"{}\",{},{}'.format(uckey, day, count))

            # Compare the predicted count to the actual count across the overlapping days.
            print('4: Calculate the error rate')
            total_count = 0
            total_pcount = 0
            total_error = 0
            found_items = 0
            for day in sorted(predicted_days.intersection(all_days)):
                key = ( uckey, day )
                if key in es_records:
                    pcount = es_records[key]
                    total_pcount += pcount
                    if key in day_count:
                        count = day_count[key]
                        if count != 0:
                            found_items += 1
                        # print('{:.3f} - {} = {:.3f}'.format(pcount, count, pcount - count))
                    else:
                        count = 0
                    error = abs(pcount - count)/(count+1)
                    total_error += error
                    total_count += count

            # Calculate the aggregate error.
            if (total_count != 0):
                agg_error = abs(total_pcount - total_count) / total_count
            else:
                agg_error = 0

            # Save some of the statistics.    
            all_uckeys.append(uckey)
            all_counts.append(total_count)
            all_pcounts.append(total_pcount)
            all_errors.append(total_error)
            all_agg_errors.append(agg_error)
            all_found.append(found_items)

        # Save this iteration time.
        all_times.append(time.time())
        all_elapsed.append(all_times[-1] - all_times[-2])

        # Print the error for all the uckeys processed so far.
        for uckey, error, found, agg_error, count, pcount in zip(all_uckeys, all_errors, all_found, all_agg_errors, all_counts, all_pcounts):
            if (found == 0):
                print('   0 : 0 : {} : {:.3f} : {}'.format(count, pcount, uckey))
            else:
                print('   {:.2f} : {:.2f} : {} : {:.3f} : {}'.format(float(error)/found, agg_error, count, pcount, uckey))

        # Write the uckeys, actual and predicted counts, and num_days data to a file.
        with open(output_filepath, 'w') as f:
            all_days_sorted = sorted(all_days)
            predicted_days_sorted = sorted(predicted_days)

            # Print the header row.
            f.write('uckey,num_days,actual_total,predicted_total')
            for day in all_days_sorted:
                f.write(',actual_{}'.format(day))
            for day in predicted_days_sorted:
                f.write(',predicted_{}'.format(day))
            f.write('\n')

            # Print the data for each uckey.
            for uckey, found, agg_error, count, pcount in zip(all_uckeys, all_found, all_agg_errors, all_counts, all_pcounts):
                f.write('"{}",{},{},{}'.format(uckey, found, count, pcount))

                # Print the actual data.
                for day in all_days_sorted:
                    key = (uckey, day)
                    if key in day_count:
                        f.write(',{}'.format(day_count[key]))
                    else:
                        f.write(',0')

                # Print the predicted data.
                for day in predicted_days_sorted:
                    key = (uckey, day)
                    if key in es_records:
                        f.write(',{}'.format(es_records[key]))
                    else:
                        f.write(',0')
                f.write('\n')


        # Print MAPE
        total = sum(all_found)
        if total != 0:
            mape = float(sum(all_errors))/total
        else:
            mape = 0

        print('average iteration:  {:.1f}'.format(sum(all_elapsed)/len(all_elapsed)))
        for i, elapsed in enumerate(all_elapsed):
            print('   iteration {} time: {:.1f}'.format(i, elapsed))
        print('total elapsed time: {:.1f}'.format(all_times[-1] - start_time))
        print('all found items:  {}'.format(total))
        print('num uckeys     :  {}'.format(len(all_uckeys)))
        print('mean abs %% error (daily): {:.3f}'.format(mape))
        print('mean abs %% error (agg  ): {:.3f}'.format(sum(all_agg_errors) / len(all_agg_errors)))

