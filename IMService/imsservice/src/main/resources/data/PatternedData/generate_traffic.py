from datetime import datetime
from datetime import timedelta
import random

# from elasticsearch import Elasticsearch
# from elasticsearch import helpers

# The followings are for Spark/Hive, comment out if there is no push to Hive
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import HiveContext

import os
import random
import sys


def read_users():
    f = open('users.txt', 'r')
    data = f.read()
    users = eval(data)
    f.close()
    return users


def calculate_loop_size(datetime_record):
    # 0 is sunday
    day_of_week_dic = {0: 5, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 3}
    hour_dic = {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 2, 7: 3, 8: 4, 9: 5, 10: 5, 11: 5, 12: 6, 13: 6, 14: 5, 15: 5,
                16: 4, 17: 4, 18: 4, 19: 4, 20: 3, 21: 3, 22: 3, 23: 2}
    holidays = ['2018-01-01', '2018-01-15', '2018-02-19', '2018-03-31', '2018-05-28', '2018-07-04', '2018-09-03',
                '2018-11-22', '2018-11-23', '2018-12-25']

    base_loop_size_for_each_user = 2

    day_of_week_factor = day_of_week_dic[datetime_record.weekday()]
    hour_factor = hour_dic[datetime_record.hour]
    holiday_str = datetime.strftime(datetime_record, '%Y-%m-%d')
    holiday_factor = 1
    if holiday_str in holidays:
        holiday_factor = 3

    return int(base_loop_size_for_each_user * day_of_week_factor * hour_factor * holiday_factor)


def is_user_active_enough(user):
    r = random.randint(1, 20)
    if (user['activity'] == 'h'):
        return True
    if (user['activity'] == 'm' and r % 2 == 0):
        return True
    if (user['activity'] == 'l' and r % 5 == 0):
        return True
    return False


def generate_traffic_for_user(user):
    traffic = user.copy()
    traffic.pop('id', None)
    traffic.pop('activity', None)
    r = random.randint(1, 20)

    # smaller si has more chance
    si = '1'
    if r % 3 == 0:
        si = '2'
    if r % 5 == 0:
        si = '3'
    traffic['si'] = si

    # if male or unknown, user uses m = magazinelock
    m = 'magazinelock'
    if user['g'] == 'g_f':
        m = 'cloudFolder'
    traffic['m'] = m_list[random.randint(0, len(m_list) - 1)]

    # More 4G usage than 3G
    t = '4G'
    if r % 3 == 0:
        t = '3G'
    traffic['t'] = t_list[random.randint(0, len(t_list) - 1)]

    return traffic


def create_new_ucdoc(uckey, traffic):
    ucdoc = traffic.copy()
    ucdoc['uckey'] = uckey
    ucdoc['days'] = {}
    return ucdoc


def create_empty_hours():
    doc = {'h0': 0, 'h1': 0, 'h2': 0, 'h3': 0, 'total': 0}
    #doc = {'total': 0}
    hours_list = []
    for _ in range(24):
        hours_list.append(doc.copy())
    return hours_list


def add_to_ucdocs(ucdocs, date, traffic):
    uckey_list = [traffic['m'], traffic['si'], traffic['t'],
                  traffic['g'], traffic['a'], 'pt', traffic['r'], 'icc']
    uckey = ','.join(uckey_list)
    date_str = datetime.strftime(date, '%Y-%m-%d')
    if uckey not in ucdocs:
        ucdoc = create_new_ucdoc(uckey, traffic)
        ucdocs[uckey] = ucdoc
    ucdoc = ucdocs[uckey]
    if date_str not in ucdoc['days']:
        ucdoc['days'][date_str] = create_empty_hours()

    ucdoc['days'][date_str][date.hour]['h0'] += 0
    ucdoc['days'][date_str][date.hour]['h1'] += 100
    ucdoc['days'][date_str][date.hour]['h2'] += 200
    ucdoc['days'][date_str][date.hour]['h3'] += 300

    #ucdoc['days'][date_str][date.hour]['total'] += 1


def push_ucdocs_to_hive(ucdocs, bucket_size):
    data_list = []
    for ucdoc in ucdocs:
        for day in ucdoc['days'].keys():
            for hour in range(24):
                uckey = ucdoc['uckey']
                bucket_id = hash(uckey) % (bucket_size)
                count_array = []
                for i in range(4):
                    count = ucdoc['days'][day][hour]["h"+str(i)]
                    cstr = str(i)+":"+str(count)
                    count_array.append(cstr)

                row = Row(uckey=uckey.decode('utf-8'),
                          bucket_id=bucket_id,
                          count_array=count_array,
                          hour=hour,
                          day=str(day))
                # print(row)
                data_list.append(row)

    df = hive_context.createDataFrame(data_list)

    # Spark 1.6
    # df.select('uckey',
    #           'bucket_id',
    #           'count_array',
    #           'hour',
    #           'day'
    #           ).write.option("header", "true").option("encoding", "UTF-8").mode('append').partitionBy("day").saveAsTable("factdata")

    # Spark 2.3
    df.select('uckey',
              'bucket_id',
              'count_array',
              'hour',
              'day'
              ).write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto("factdata")


if __name__ == '__main__':

    users = read_users()

    start_date_str = '2018-01-01'
    end_date_str = '2018-02-01'
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    m_list = ['cloudFolder', 'magazinelock', 'minusonepage']
    t_list = ['3G', '4G', 'Wifi', '5G']

    ucdocs = {}

    # Loop over time
    date = start_date
    while date < end_date:
        loop_size = calculate_loop_size(date)
        print(date)

        for _ in range(loop_size):
            for user in users:
                if is_user_active_enough(user):
                    traffic = generate_traffic_for_user(user)
                    add_to_ucdocs(ucdocs, date, traffic)

        date = date + timedelta(hours=1)

    # The following piece pushes the ucdocs to ES or Hive
    push_to_es = False
    if push_to_es:

        es_host = "10.193.217.111"
        es_port = 9200
        es_index = 'predictions_test_generated_11142019'
        es_type = 'doc'
        es = Elasticsearch([{'host': es_host, 'port': es_port}], timeout=600)

        print("Pushing docs:" + str(len(ucdocs.values())))
        actions = []
        for ucdoc in ucdocs.values():
            one_doc = {'_index': es_index, '_type': es_type,
                       'uckey': ucdoc['uckey'], 'ucdoc': ucdoc}
            actions.append(one_doc)
        helpers.bulk(es, actions)
    else:
        sc = SparkContext()
        hive_context = HiveContext(sc)

        command = """
        DROP TABLE IF EXISTS factdata
        """
        hive_context.sql(command)

        command = """
        CREATE TABLE IF NOT EXISTS factdata
        (
        uckey string,
        bucket_id int,
        count_array array<string>,
        hour int
        )
        partitioned by (day string)
        """
        hive_context.setConf("hive.exec.dynamic.partition", "true")
        hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        hive_context.sql(command)

        bucket_size = 64
        push_ucdocs_to_hive(ucdocs.values(), bucket_size)
