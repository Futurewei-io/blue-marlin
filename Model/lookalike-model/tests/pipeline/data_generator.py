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

import sys
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, ArrayType, LongType
from lookalike_model.pipeline.main_clean import add_aid_bucket
from lookalike_model.pipeline.util import write_to_table

#==========================================
# Create Hive tables for the unit tests
#==========================================
# Creates raw persona data and writes it to Hive.
def create_persona_table (spark, table_name):
    df = create_raw_persona(spark)
    df = df.withColumnRenamed('gender', 'gender_new_dev')
    df = df.withColumnRenamed('age', 'forecast_age_dev')
    df.printSchema()
    write_to_table(df, table_name)

# Creates cleaned persona data and writes it to Hive.
def create_log_table (spark, table_name):
    df = create_cleaned_persona(spark)
    write_to_table(df, table_name)

# Creates raw clicklog data and writes it to Hive.
def create_clicklog_table (spark, table_name):
    df = create_raw_log(spark)
    df = df.withColumnRenamed('media', 'adv_type')
    df = df.withColumnRenamed('price_model', 'adv_bill_mode_cd')
    df = df.withColumnRenamed('action_time', 'click_time')
    df.printSchema()
    write_to_table(df, table_name)

# Creates raw showlog data and writes it to Hive.
def create_showlog_table (spark, table_name):
    df = create_raw_log(spark)
    df = df.withColumnRenamed('media', 'adv_type')
    df = df.withColumnRenamed('price_model', 'adv_bill_mode_cd')
    df = df.withColumnRenamed('action_time', 'show_time')
    df.printSchema()
    write_to_table(df, table_name)

# Creates cleaned click/showlog data and writes it to Hive.
def create_log_table (spark, table_name):
    df = create_cleaned_log(spark)
    write_to_table(df, table_name)

# Creates keyword data and writes it to Hive.
def create_keywords_table (spark, table_name):
    df = create_keywords(spark)
    write_to_table(df, table_name)

# Creates unified log data and writes it to Hive.
def create_unified_log_table (spark, table_name):
    df = create_unified_log(spark)
    write_to_table(df, table_name)

# Creates raw clicklog data and writes it to Hive.
def create_keywords_showlog_table (spark, table_name):
    df = create_keywords_raw_log(spark)
    df = df.withColumnRenamed('media', 'adv_type')
    df = df.withColumnRenamed('price_model', 'adv_bill_mode_cd')
    df = df.withColumnRenamed('action_time', 'show_time')
    df.printSchema()
    write_to_table(df, table_name)

# Creates the effective keywords data and writes is to Hive.
def create_effective_keywords_table(spark, table_name):
    df = create_effective_keywords(spark)
    write_to_table(df, table_name)

#==========================================
# Create dataframes for the unit tests
#==========================================
# Returns a dataframe with unclean persona data.
def create_raw_persona (spark):
    # Create a data set with duplicate entries and non-duplicate entries with the same did.
    data = [
        ('0000001', 0, 0), # duplicate entry, duplicates will be removed
        ('0000001', 0, 0),
        ('0000001', 0, 0),
        ('0000001', 0, 0),
        ('0000002', 1, 0), # duplicate entry, duplicates will be removed
        ('0000002', 1, 0),
        ('0000002', 1, 0),
        ('0000002', 1, 0),
        ('0000003', 0, 1), # duplicate entry, duplicates will be removed
        ('0000003', 0, 1),
        ('0000003', 0, 1),
        ('0000003', 0, 1),
        ('0000004', 1, 1),
        ('0000005', 0, 2),
        ('0000006', 1, 2),
        ('0000007', 0, 3),
        ('0000008', 1, 3),
        ('0000009', 0, 4),
        ('0000010', 1, 4),
        ('0000011', 0, 2), # repeated did with conflicting age and gender, will be dropped
        ('0000011', 1, 3),
        ('0000011', 2, 4),
    ]

    schema = StructType([
        StructField("aid", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)

# Returns a dataframe with cleaned persona data.
def create_cleaned_persona (spark, aid_bucket_num = 4):
    data = [
        ('0000001', 0, 0),
        ('0000002', 1, 0),
        ('0000003', 0, 1),
        ('0000004', 1, 1),
        ('0000005', 0, 2),
        ('0000006', 1, 2),
        ('0000007', 0, 3),
        ('0000008', 1, 3),
        ('0000009', 0, 4),
        ('0000010', 1, 4)
    ]

    schema = StructType([
        StructField("aid", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    return add_aid_bucket(spark.createDataFrame(spark.sparkContext.parallelize(data), schema), aid_bucket_num)

# Returns a dataframe with unclean log data.
def create_raw_log (spark):
    data = [
        ('0000001', '1000', 'splash', 'abcdef0', 'C000', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-01 12:34:56.78'),
        ('0000002', '1000', 'splash', 'abcdef1', 'C001', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-02 12:34:56.78'),
        ('0000003', '1001', 'native', 'abcdef2', 'C002', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'),
        ('0000004', '1001', 'native', 'abcdef3', 'C010', 'ABC-AL00',   '4G', 'CPD', '2020-01-04 12:34:56.78'),
        ('0000005', '1002', 'splash', 'abcdef4', 'C011', 'DEF-AL00', 'WIFI', 'CPM', '2020-01-05 12:34:56.78'),
        ('0000006', '1002', 'splash', 'abcdef5', 'C012', 'DEF-AL00', 'WIFI', 'CPM', '2020-01-06 12:34:56.78'),
        ('0000007', '1003', 'splash', 'abcdef6', 'C020', 'XYZ-AL00',   '4G', 'CPT', '2020-01-07 12:34:56.78'),
        ('0000008', '1003', 'splash', 'abcdef7', 'C021', 'XYZ-AL00',   '4G', 'CPT', '2020-01-08 12:34:56.78'),
        ('0000009', '1004', 'splash', 'abcdef8', 'C022', 'TUV-AL00', 'WIFI', 'CPC', '2020-01-09 12:34:56.78'),
        ('0000010', '1004', 'splash', 'abcdef9', 'C023', 'TUV-AL00', 'WIFI', 'CPC', '2020-01-10 12:34:56.78'),
        ('0000001', '1000', 'native', 'abcde10', 'C004', 'JKL-AL00',   '4G', 'CPD', '2020-01-11 12:34:56.78'), # Slot ID not in list so will be filtered.
    ]

    schema = StructType([
        StructField("aid", StringType(), True),
        StructField("adv_id", StringType(), True),
        StructField("media", StringType(), True),
        StructField("slot_id", StringType(), True),
        StructField("spread_app_id", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("net_type", StringType(), True),
        StructField("price_model", StringType(), True),
        StructField("action_time", StringType(), True)
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with cleaned log data.
def create_cleaned_log (spark):
    data = [
        ('C000', '0000001', '1000', 'splash', 'abcdef0', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-01 12:34:56.78', 'Huawei Magazine', 0, 0, 'travel', '1', '2020-01-01', '1', ),
        ('C001', '0000002', '1000', 'splash', 'abcdef1', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-02 12:34:56.78', 'Huawei Browser', 1, 0, 'travel', '1', '2020-01-02', '1', ),
        ('C002', '0000003', '1001', 'native', 'abcdef2', 'ABC-AL00', '4G', 'CPD', '2020-01-03 12:34:56.78', 'Huawei Video', 0, 1, 'travel', '1', '2020-01-03', '1', ),
        ('C010', '0000004', '1001', 'native', 'abcdef3', 'ABC-AL00', '4G', 'CPD', '2020-01-04 12:34:56.78', 'Huawei Music', 1, 1, 'game-avg', '2', '2020-01-04', '1', ),
        ('C011', '0000005', '1002', 'splash', 'abcdef4', 'DEF-AL00', 'WIFI', 'CPM', '2020-01-05 12:34:56.78', 'Huawei Reading', 0, 2, 'game-avg', '2', '2020-01-05', '1', ),
        ('C012', '0000006', '1002', 'splash', 'abcdef5', 'DEF-AL00', 'WIFI', 'CPM', '2020-01-06 12:34:56.78', 'Huawei Magazine', 1, 2, 'game-avg', '2', '2020-01-06', '0', ),
        ('C020', '0000007', '1003', 'splash', 'abcdef6', 'XYZ-AL00', '4G', 'CPT', '2020-01-07 12:34:56.78', 'Huawei Browser', 0, 3, 'reading', '3', '2020-01-07', '0', ),
        ('C021', '0000008', '1003', 'splash', 'abcdef7', 'XYZ-AL00', '4G', 'CPT', '2020-01-08 12:34:56.78', 'Huawei Video', 1, 3, 'reading', '3', '2020-01-08', '0', ),
        ('C022', '0000009', '1004', 'splash', 'abcdef8', 'TUV-AL00', 'WIFI', 'CPC', '2020-01-09 12:34:56.78', 'Huawei Music', 0, 4, 'reading', '3', '2020-01-09', '0', ),
        ('C023', '0000010', '1004', 'splash', 'abcdef9', 'TUV-AL00', 'WIFI', 'CPC', '2020-01-10 12:34:56.78', 'Huawei Reading', 1, 4, 'reading', '3', '2020-01-10', '1', ),
    ]

    schema = StructType([
        StructField('spread_app_id', StringType(), True),
        StructField('aid', StringType(), True),
        StructField('adv_id', StringType(), True),
        StructField('media', StringType(), True),
        StructField('slot_id', StringType(), True),
        StructField('device_name', StringType(), True),
        StructField('net_type', StringType(), True),
        StructField('price_model', StringType(), True),
        StructField('action_time', StringType(), True),
        StructField('media_category', StringType(), True),
        StructField('gender', IntegerType(), True),
        StructField('age', IntegerType(), True),
        StructField('keyword', StringType(), True),
        StructField('keyword_index', StringType(), True),
        StructField('day', StringType(), True),
        StructField('aid_bucket', StringType(), True),
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with keyword data.
def create_keywords(spark):
    data = [
        ('travel', 'C000', 1),
        ('travel', 'C001', 1),
        ('travel', 'C002', 1),
        ('travel', 'C003', 1),
        ('travel', 'C004', 1),
        ('game-avg', 'C010', 2),
        ('game-avg', 'C011', 2),
        ('game-avg', 'C012', 2),
        ('game-avg', 'C013', 2),
        ('game-avg', 'C014', 2),
        ('reading', 'C020', 3),
        ('reading', 'C021', 3),
        ('reading', 'C022', 3),
        ('reading', 'C023', 3),
        ('reading', 'C024', 3),
        ('shopping', 'C030', 4),
        ('shopping', 'C031', 4),
        ('shopping', 'C032', 4),
        ('shopping', 'C033', 4),
        ('shopping', 'C034', 4),
        ('education', 'C040', 5),
        ('education', 'C041', 5),
        ('education', 'C042', 5),
        ('education', 'C043', 5),
        ('education', 'C044', 5),
    ]

    schema = StructType([
        StructField("keyword", StringType(), True),
        StructField("spread_app_id", StringType(), True),
        StructField("keyword_index", StringType(), True)
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with a unified log data.
def create_unified_log (spark):


    data = [
        ('0000001', 0, '2020-01-01 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-01 12:34:56.78', 'game-avg', '2', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-01 12:34:56.78', 'game-avg', '2', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'game-avg', '2', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'game-avg', '2', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-01 12:34:56.78', 'reading', '3', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-01 12:34:56.78', 'reading', '3', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-01 12:34:56.78', 'reading', '3', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 1, '2020-01-01 12:34:56.78', 'reading', '3', 'splash', 'WIFI', 0, 0, '1000', 1577836800, 1577910896, '2020-01-01', '1', ),
        ('0000001', 0, '2020-01-02 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577923200, 1577997296, '2020-01-02', '1', ),
        ('0000001', 1, '2020-01-02 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 0, 0, '1000', 1577923200, 1577997296, '2020-01-02', '1', ),
        ('0000001', 0, '2020-01-03 12:34:56.78', 'travel', '1', 'native', '4G', 0, 0, '1001', 1578009600, 1578083696, '2020-01-03', '1', ),
        ('0000001', 1, '2020-01-03 12:34:56.78', 'travel', '1', 'native', '4G', 0, 0, '1001', 1578009600, 1578083696, '2020-01-03', '1', ),
        ('0000002', 0, '2020-01-02 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 1, 0, '1000', 1577923200, 1577997296, '2020-01-02', '1', ),
        ('0000002', 0, '2020-01-02 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 1, 0, '1000', 1577923200, 1577997296, '2020-01-02', '1', ),
        ('0000002', 0, '2020-01-02 12:34:56.78', 'travel', '1', 'splash', 'WIFI', 1, 0, '1000', 1577923200, 1577997296, '2020-01-02', '1', ),
        ('0000003', 1, '2020-01-03 12:34:56.78', 'travel', '1', 'native', '4G', 0, 1, '1001', 1578009600, 1578083696, '2020-01-03', '1', ),
        ('0000003', 1, '2020-01-03 12:34:56.78', 'travel', '1', 'native', '4G', 0, 1, '1001', 1578009600, 1578083696, '2020-01-03', '1', ),
        ('0000003', 1, '2020-01-03 12:34:56.78', 'travel', '1', 'native', '4G', 0, 1, '1001', 1578009600, 1578083696, '2020-01-03', '1', ),
    ]

    schema = StructType([
        StructField('aid', StringType(), True),
        StructField('is_click', IntegerType(), True),
        StructField('action_time', StringType(), True),
        StructField('keyword', StringType(), True),
        StructField('keyword_index', StringType(), True),
        StructField('media', StringType(), True),
        StructField('net_type', StringType(), True),
        StructField('gender', IntegerType(), True),
        StructField('age', IntegerType(), True),
        StructField('adv_id', StringType(), True),
        StructField('interval_starting_time', IntegerType(), True),
        StructField('action_time_seconds', IntegerType(), True),
        StructField('day', StringType(), True),
        StructField('aid_bucket', StringType(), True),
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with trainready data.
def create_trainready_data (spark):
    data = [
        (0, 0, '0000001', 1000000001, [u'1578009600', u'1577923200', u'1577836800'], [u'travel', u'travel', u'travel,game-avg'], [u'1', u'1', u'1,2'], [u'1:2', u'1:2', u'1:2,2:1'], [u'1:1', u'1:1', u'1:1,2:0'], '1', ),
        (0, 1, '0000002', 1000000002, [u'1577923200'], [u'travel'], [u'1'], [u'1:2'], [u'1:1'], '1', ),
        (1, 0, '0000003', 1000000003, [u'1578009600'], [u'travel'], [u'1'], [u'1:2'], [u'1:1'], '1', ),
        (1, 1, '0000004', 1000000004, [u'1578096000'], [u'game-avg'], [u'2'], [u'2:2'], [u'2:1'], '1', ),
        (2, 0, '0000005', 1000000005, [u'1578182400'], [u'game-avg'], [u'2'], [u'2:2'], [u'2:1'], '1', ),
        (2, 1, '0000006', 1, [u'1578268800'], [u'game-avg'], [u'2'], [u'2:2'], [u'2:1'], '0', ),
        (3, 0, '0000007', 2, [u'1578355200'], [u'reading'], [u'3'], [u'3:2'], [u'3:1'], '0', ),
        (3, 1, '0000008', 3, [u'1578441600'], [u'reading'], [u'3'], [u'3:2'], [u'3:1'], '0', ),
        (4, 0, '0000009', 4, [u'1578528000'], [u'reading'], [u'3'], [u'3:2'], [u'3:1'], '0', ),
        (4, 1, '0000010', 1000000006, [u'1578614400'], [u'reading'], [u'3'], [u'3:2'], [u'3:1'], '1', ),
    ]

    schema = StructType([
        StructField('age', IntegerType(), True),
        StructField('gender', IntegerType(), True),
        StructField('aid', StringType(), True),
        StructField('aid_index', LongType(), True),
        StructField('interval_starting_time', ArrayType(StringType(), True), True),
        StructField('interval_keywords', ArrayType(StringType(), True), True),
        StructField('kwi', ArrayType(StringType(), True), True),
        StructField('kwi_show_counts', ArrayType(StringType(), True), True),
        StructField('kwi_click_counts', ArrayType(StringType(), True), True),
        StructField('aid_bucket', StringType(), True),
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with trainready data for testing user filtering.
def create_trainready_filter_user_data (spark):
    data = [
        (0, 0, 'normal', 1, 
            [u'1578009600', u'1577923200', u'1577836800', u'1577750400', u'157766400', u'1577577600', u'1577491200', u'1577404800', u'1577318400', u'1577232000'], 
            [u'travel', u'travel',u'travel', u'travel',u'travel', u'travel', u'game-avg',u'travel', u'travel', u'travel,game-avg'], 
            [u'1', u'1', u'1', u'1', u'1', u'1', u'1',u'1', u'1', u'1,2'], 
            [u'1:1', u'1:1', u'1:1', u'1:1', u'1:2', u'1:2', u'2:1', u'1:2', u'1:2', u'1:2,2:1'], # 16
            [u'1:0', u'1:0', u'1:0', u'1:0', u'1:1', u'1:1', u'2:0', u'1:1', u'1:1', u'1:1,2:0'], '1', ),
        (0, 0, 'low average show count/few active intervals', 2, 
            [u'1578009600', u'1577923200', u'1577836800', u'1577750400', u'157766400', u'1577577600', u'1577491200', u'1577404800', u'1577318400', u'1577232000'], 
            [u'travel', u'travel',u'travel', u'travel',u'travel', u'travel', u'game-avg',u'travel', u'travel', u'travel,game-avg'], 
            [u'1', u'1', u'1', u'1', u'1', u'1', u'1',u'1', u'1', u'1,2'], 
            [u'1:0', u'1:0', u'1:0', u'1:0', u'1:0', u'1:0', u'2:0', u'1:0', u'1:0', u'1:0,2:1'], # 1
            [u'1:0', u'1:0', u'1:0', u'1:0', u'1:1', u'1:1', u'2:0', u'1:1', u'1:1', u'1:1,2:0'], '1', ),
        (0, 0, 'high average show count', 3, 
            [u'1578009600', u'1577923200', u'1577836800', u'1577750400', u'157766400', u'1577577600', u'1577491200', u'1577404800', u'1577318400', u'1577232000'], 
            [u'travel', u'travel',u'travel', u'travel',u'travel', u'travel', u'game-avg',u'travel', u'travel', u'travel,game-avg'], 
            [u'1', u'1', u'1', u'1', u'1', u'1', u'1',u'1', u'1', u'1,2'], 
            [u'1:5000', u'1:0', u'1:0', u'1:0', u'1:0', u'1:0', u'2:0', u'1:0', u'1:0', u'1:1,2:1'], # 5002
            [u'1:0', u'1:0', u'1:0', u'1:0', u'1:1', u'1:1', u'2:0', u'1:1', u'1:1', u'1:1,2:0'], '1', ),
        (0, 0, 'sparse impressions', 4, 
            [u'1577232000'], 
            [u'travel,game-avg'], 
            [u'1,2'], 
            [u'1:10,2:10'], # 20
            [u'1:1,2:0'], '1', ),
    ]

    schema = StructType([
        StructField('age', IntegerType(), True),
        StructField('gender', IntegerType(), True),
        StructField('aid', StringType(), True),
        StructField('aid_index', LongType(), True),
        StructField('interval_starting_time', ArrayType(StringType(), True), True),
        StructField('interval_keywords', ArrayType(StringType(), True), True),
        StructField('kwi', ArrayType(StringType(), True), True),
        StructField('kwi_show_counts', ArrayType(StringType(), True), True),
        StructField('kwi_click_counts', ArrayType(StringType(), True), True),
        StructField('aid_bucket', StringType(), True),
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with unclean log data.
def create_keywords_raw_log (spark):
    data = [
        ('0000001', '1000', 'splash', 'abcdef0', 'C000', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-01 12:34:56.78'), # travel
        ('0000002', '1000', 'splash', 'abcdef1', 'C001', 'DUB-AL00', 'WIFI', 'CPC', '2020-01-02 12:34:56.78'), # travel
        ('0000003', '1001', 'native', 'abcdef2', 'C002', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000005', '1001', 'native', 'abcdef2', 'C002', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000006', '1001', 'native', 'abcdef2', 'C002', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000007', '1001', 'native', 'abcdef2', 'C002', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000008', '1001', 'native', 'abcdef2', 'C003', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000009', '1001', 'native', 'abcdef2', 'C003', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000010', '1001', 'native', 'abcdef2', 'C003', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000011', '1001', 'native', 'abcdef2', 'C004', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000012', '1001', 'native', 'abcdef2', 'C004', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000013', '1001', 'native', 'abcdef2', 'C004', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # travel
        ('0000014', '1001', 'native', 'abcdef2', 'C010', 'ABC-AL00',   '4G', 'CPD', '2020-01-03 12:34:56.78'), # game-avg
        ('0000004', '1001', 'native', 'abcdef3', 'C010', 'ABC-AL00',   '4G', 'CPD', '2020-01-04 12:34:56.78'), # game-avg
        ('0000005', '1001', 'native', 'abcdef3', 'C010', 'ABC-AL00',   '4G', 'CPD', '2020-01-05 12:34:56.78'), # game-avg
        ('0000007', '1003', 'splash', 'abcdef6', 'C020', 'XYZ-AL00',   '4G', 'CPT', '2020-01-07 12:34:56.78'), # reading; only one entry for this keyword so will be excluded.
        ('0000008', '1003', 'splash', 'abcdef6', 'C030', 'XYZ-AL00',   '4G', 'CPT', '2020-01-08 12:34:56.78'), # shopping; only one entry for this keyword so will be excluded.
        ('0000009', '1003', 'splash', 'abcdef6', 'C040', 'XYZ-AL00',   '4G', 'CPT', '2020-01-09 12:34:56.78'), # education; just enough entries to be included.
        ('0000009', '1003', 'splash', 'abcdef6', 'C040', 'XYZ-AL00',   '4G', 'CPT', '2020-01-09 12:34:56.78'), # education; just enough entries to be included.
        ('0000010', '1003', 'splash', 'abcdef6', 'C050', 'XYZ-AL00',   '4G', 'CPT', '2020-01-10 12:34:56.78'), # no mapping; only one entry for this keyword so will be excluded.
        ('0000001', '1000', 'native', 'abcde10', 'C020', 'JKL-AL00',   '4G', 'CPD', '2020-01-11 12:34:56.78'), # reading; outside the date range.
    ]

    schema = StructType([
        StructField("aid", StringType(), True),
        StructField("adv_id", StringType(), True),
        StructField("media", StringType(), True),
        StructField("slot_id", StringType(), True),
        StructField("spread_app_id", StringType(), True),
        StructField("device_name", StringType(), True),
        StructField("net_type", StringType(), True),
        StructField("price_model", StringType(), True),
        StructField("action_time", StringType(), True)
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with the effective keyword data.
def create_effective_keywords(spark):
    data = [('travel',), ('game-avg',), ('education',)]
    schema = StructType([
        StructField("keyword", StringType(), True)
    ])
    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)


# Prints to screen the code to generate the given data frame.
def print_df_generator_code (df):
    columns = df.columns
    print
    print('    data = [')
    for row in df.collect():
        sys.stdout.write('        (')
        for column in columns:
            if isinstance(df.schema[column].dataType, StringType):
                sys.stdout.write('\'%s\', ' % row[column])
            else:
                sys.stdout.write('%s, ' % row[column])
        print('),')
    print('    ]')
    print
    print('    schema = StructType([')
    for column in columns:
        print('        StructField(\'%s\', %s(), True),' % (column, type(df.schema[column].dataType).__name__))
    print('    ])')
    print



