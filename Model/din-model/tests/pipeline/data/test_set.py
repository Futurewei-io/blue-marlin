# Copyright 2020, Futurewei Technologies
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
"""
data file for data preparation of running unit tests.
"""
from pyspark.sql import types
from pyspark.sql import Row

keyword_columns = ['spread_app_id', 'keyword', 'keyword_index']
keyword_tested = [
    ('C10499309', 'video', 1),
    ('C10295678', 'info', 2)
]

persona_columns = ['did', 'gender', 'age']
persona_tested = [
    ('1000', '0', '1'),
    ('1001', '1', '2'),
    ('1002', '0', '3'),
    ('1000', '0', '4')
]
persona_expected = [
    ('1001', 1, 2),
    ('1002', 0, 3)
]
persona_cleaned = [
    ('1001', 1, 2),
    ('1002', 0, 3),
    ('1004', 1, 3)
]

log_columns_origin = ['did', 'adv_id', 'adv_type', 'slot_id', 'spread_app_id',
                      'device_name', 'net_type', 'adv_bill_mode_cd']
log_show_columns = log_columns_origin + ['show_time']
log_click_columns = log_columns_origin + ['click_time']
log_columns_syned = ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id',
                     'device_name', 'net_type', 'price_model', 'action_time']
log_columns_cleaned = ['did', 'adv_id', 'media', 'slot_id', 'spread_app_id',
                       'media_category', 'device_name', 'net_type', 'price_model',
                       'action_time', 'gender', 'age', 'keyword', 'keyword_index']
log_show_tested = [
    ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113'),
    ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413'),
    ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213'),
    ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613'),
    ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613')
]
log_show_tested_2 = [
    ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113'),
    ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413'),
    ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213'),
    ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613'),
    ('1001', '103364', 'native', 'a47eavw7ey', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-06 08:05:00.613'),
    ('1003', '103364', 'native', 'a47eavw7ex', 'C10499309',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:05:00.613')
]
log_show_expected = [
    ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113', 1, 2, 'video', 1),
    ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413', 0, 3, 'info', 2),
    ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213', 1, 2, 'info', 2)
]
log_click_tested = log_show_tested
log_click_tested_2 = log_show_tested_2

log_show_to_be_joined = [
    ('1001', '103364', 'native', 'a47eavw7ex', 'C10499309', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.113', 1, 2, 'video', 1),
    ('1002', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-02 08:03:46.413', 0, 3, 'info', 2)
]
log_click_to_be_joined = [
    ('1001', '45022046', 'native', 'a47eavw7ex', 'C10295678', 'AI assistant',
     'LLD-AL20', 'WIFI', 'CPC', '2020-04-03 08:04:06.213', 1, 2, 'info', 2)
]

log_union_columns = ['did', 'is_click', 'action_time', 'keyword', 'keyword_index',
                     'media', 'media_category', 'net_type', 'gender', 'age', 'adv_id']
batch_config = ('2019-12-19', '2020-04-15', 144000)
interval_time_in_seconds = 86400
log_union_interval_columns = log_union_columns + \
    ['action_time_seconds', 'interval_starting_time', 'uckey']
log_union_interval_expected = [
    ('1001', 0, '2020-04-02 08:03:46.113', 'video', 1,
     'native', 'AI assistant', 'WIFI', 1, 2, '103364', 1585839826, 1585785600, 'native,AI assistant,WIFI,1,2'),
    ('1002', 0, '2020-04-02 08:03:46.413', 'info', 2,
     'native', 'AI assistant', 'WIFI', 0, 3, '45022046', 1585839826, 1585785600, 'native,AI assistant,WIFI,0,3'),
    ('1001', 1, '2020-04-03 08:04:06.213', 'info', 2,
     'native', 'AI assistant', 'WIFI', 1, 2, '45022046', 1585926246, 1585872000, 'native,AI assistant,WIFI,1,2')
]

trainready_logs_columns = ["did", "is_click", "action_time", "keyword", "keyword_index", 
                          "media", "media_category", "net_type", "gender", "age", "adv_id", 
                          "action_time_seconds", "interval_starting_time", "uckey", "region_id"]
trainready_logs_tested = [
   ('3eca6372fdb5a0ababd8f9bc3bf4f241dbe39ce1546ea3efb8efdce172a9fe22', 0, 
   '2020-04-11 07:02:26.168', 'reading', 24, 'native', 'Huawei Browser', '4G', 
   0, 3, '45034109', 1586613746, 1586563200, 'native,Huawei Browser,4G,0,3,81', 81), 
   ('3eca6372fdb5a0ababd8f9bc3bf4f241dbe39ce1546ea3efb8efdce172a9fe22', 0, 
   '2020-04-11 01:07:39.889', 'reading', 24, 'native', 'Huawei Browser', '4G', 
   0, 3, '45033921', 1586592459, 1586563200, 'native,Huawei Browser,4G,0,3,81', 81), 
   ('44927efedec6d9353f5122e6566d12161ff67edaab9eff35df61a86076f0a58e', 0, 
   '2020-04-14 06:20:42.377', 'reading', 24, 'native', 'Huawei Browser', '4G', 
   0, 3, '45033921', 1586870442, 1586822400, 'native,Huawei Browser,4G,0,3,81', 81), 
   ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, 
   '2020-04-08 20:29:20.114', 'video', 29, 'roll', 'Huawei Video', '4G', 
   0, 4, '45030499', 1586402960, 1586390400, 'roll,Huawei Video,4G,0,4,82', 82), 
   ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, 
   '2020-04-13 12:20:30.899', 'video', 29, 'roll', 'Huawei Video', '4G', 
   0, 4, '45034378', 1586805630, 1586736000, 'roll,Huawei Video,4G,0,4,82', 82), 
   ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, 
   '2020-04-09 00:47:01.142', 'video', 29, 'roll', 'Huawei Video', '4G', 
   0, 4, '45030868', 1586418421, 1586390400, 'roll,Huawei Video,4G,0,4,82', 82),
   ('d5c75d1a92f0531db61d39cc5a5fe5c6d98d892327b145ae4fcc74222a39029e', 0, 
   '2020-04-11 18:59:30.550', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 
   1, 3, '45024950', 1586656770, 1586649600, 'splash,Huawei Reading,WIFI,1,3,51', 51), 
   ('d5c75d1a92f0531db61d39cc5a5fe5c6d98d892327b145ae4fcc74222a39029e', 0, 
   '2020-04-01 18:34:25.112', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 
   1, 3, '45025925', 1585791265, 1585785600, 'splash,Huawei Reading,WIFI,1,3,51', 51), 
   ('3a8b7621165f117eccdf6137d679dad5fd4ef89ae0f023aa75a60d3520725580', 0, 
   '2020-03-28 11:06:47.168', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 
   1, 3, '45027548', 1585418807, 1585353600, 'splash,Huawei Reading,WIFI,1,3,51', 51)
]
trainready_logs_expected_columns = [
    'region_id', 'age', 'gender', 'net_type', 'media_category', 'media', 'uckey',
    'interval_starting_time', 'keyword_indexes', 'keyword_indexes_click_counts',
    'keyword_indexes_show_counts', 'keywords', 'keywords_click_counts',
    'keywords_show_counts', 'uckey_index', 'media_index', 'media_category_index', 
    'net_type_index', 'gender_index', 'age_index', 'region_id_index']
trainready_logs_expected = [
   (51, 3, 1, 'WIFI', 'Huawei Reading', 'splash', 'splash,Huawei Reading,WIFI,1,3,51', 
   [1586649600, 1585785600, 1585353600], ['29', '29', '29'], ['29:0', '29:0', '29:0'], 
   ['29:1', '29:1', '29:1'], ['video', 'video', 'video'], ['video:0', 'video:0', 'video:0'], 
   ['video:1', 'video:1', 'video:1'], 3, 3, 2, 2, 2, 1, 1), 
   (81, 3, 0, '4G', 'Huawei Browser', 'native', 'native,Huawei Browser,4G,0,3,81', 
   [1586822400, 1586563200], ['24', '24'], ['24:0', '24:0'], 
   ['24:1', '24:2'], ['reading', 'reading'], ['reading:0', 'reading:0'], 
   ['reading:1', 'reading:2'], 1, 1, 1, 1, 1, 1, 2), 
   (82, 4, 0, '4G', 'Huawei Video', 'roll', 'roll,Huawei Video,4G,0,4,82', 
   [1586736000, 1586390400], ['29', '29'], ['29:0', '29:0'], 
   ['29:1', '29:2'], ['video', 'video'], ['video:0', 'video:0'], 
   ['video:1', 'video:2'], 2, 2, 3, 1, 1, 2, 3)
]

trainready_schema = [
    ('region_id', 'int'), ('age', 'int'), ('gender', 'int'), 
    ('net_type', 'string'), ('media_category', 'string'), 
    ('media', 'string'), ('uckey', 'string'), 
    ('interval_starting_time', 'array<int>'), 
    ('keyword_indexes', 'array<string>'), 
    ('keyword_indexes_click_counts', 'array<string>'), 
    ('keyword_indexes_show_counts', 'array<string>'), 
    ('keywords', 'array<string>'), 
    ('keywords_click_counts', 'array<string>'), 
    ('keywords_show_counts', 'array<string>'), 
    ('uckey_index', 'int'), ('media_index', 'int'), 
    ('media_category_index', 'int'), ('net_type_index', 'int'), 
    ('gender_index', 'int'), ('age_index', 'int'), 
    ('region_id_index', 'int')
]

logs_with_regions_schema = types.StructType(
    [types.StructField('uckey', types.StringType())]
)

logs_with_regions_output_schema = types.StructType(
    [types.StructField('uckey', types.StringType()),
     types.StructField('index', types.IntegerType())])

logs_with_regions_test = [
    Row(uckey='native,Huawei Music,WIFI,0,4,1'),
    Row(uckey='native,Huawei Music,WIFI,0,4,1'),
    Row(uckey='native,Huawei Music,WIFI,0,4,1'),
    Row(uckey='native,Huawei Music,WIFI,0,4,1'),

    Row(uckey='native,Huawei Brower,3G,1,3,40'),

    Row(uckey='native,Huawei Brower,3G,1,3,18'),
    Row(uckey='native,Huawei Brower,3G,1,3,18'),
]

logs_with_regions_expected = [
    Row(uckey='native,Huawei Music,WIFI,0,4,1', index=28),
    Row(uckey='native,Huawei Music,WIFI,0,4,1', index=69),
    Row(uckey='native,Huawei Music,WIFI,0,4,1', index=77),
    Row(uckey='native,Huawei Music,WIFI,0,4,1', index=1),

    Row(uckey='native,Huawei Brower,3G,1,3,40', index=1),

    Row(uckey='native,Huawei Brower,3G,1,3,18', index=69),
    Row(uckey='native,Huawei Brower,3G,1,3,18', index=1),
]

add_region_to_logs_df_schema = types.StructType(
    [
        types.StructField('uckey', types.StringType()),
        types.StructField('region_id', types.IntegerType()),
        types.StructField('media', types.StringType()),
        types.StructField('media_category', types.StringType()),
        types.StructField('net_type', types.StringType()),
        types.StructField('gender', types.IntegerType()),
        types.StructField('age', types.IntegerType()),
        types.StructField('action_time', types.StringType()),
    ]
)

add_region_to_logs_rows = [
    Row(uckey='', region_id=-1, media='m1', media_category='mc1', net_type='nt1', gender=0, age=18, action_time='2020-03-18 10:00:00'),
    Row(uckey='', region_id=-1, media='m2', media_category='mc2', net_type='nt2', gender=1, age=20, action_time='2020-03-18 10:00:00')
]

add_region_to_logs_df_expected_schema = types.StructType(
    [
        types.StructField('uckey', types.StringType()),
        types.StructField('region_id', types.IntegerType()),
        types.StructField('media', types.StringType()),
        types.StructField('media_category', types.StringType()),
        types.StructField('net_type', types.StringType()),
        types.StructField('gender', types.IntegerType()),
        types.StructField('age', types.IntegerType()),
        types.StructField('action_time', types.StringType()),
    ]
)

add_region_to_logs_rows_expected = [
    Row(uckey='', region_id=-1, media='m1', media_category='mc1', net_type='nt1', gender=0, age=18, action_time='2020-03-18 10:00:00'),
    Row(uckey='', region_id=-1, media='m2', media_category='mc2', net_type='nt2', gender=1, age=20, action_time='2020-03-18 10:00:00')
]

