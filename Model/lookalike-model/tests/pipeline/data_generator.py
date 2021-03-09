# Copyright 2021, Futurewei Technologies
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
import sys
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from lookalike_model.pipeline.main_clean import add_did_bucket
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
        StructField("did", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", StringType(), True)
    ])

    return spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

# Returns a dataframe with cleaned persona data.
def create_cleaned_persona (spark, bucket_num = 4):
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
        StructField("did", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    return add_did_bucket(spark.createDataFrame(spark.sparkContext.parallelize(data), schema), bucket_num)

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
        StructField("did", StringType(), True),
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
        StructField('did', StringType(), True),
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
        StructField('did_bucket', StringType(), True),
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
        ('reading', 'C024', 3)
    ]

    schema = StructType([
        StructField("keyword", StringType(), True),
        StructField("spread_app_id", StringType(), True),
        StructField("keyword_index", StringType(), True)
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



