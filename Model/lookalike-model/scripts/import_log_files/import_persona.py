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


'''
This file load data into a temperary table and create a persona table out of it.

spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict import_factdata_files_1.py

'''


from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import when, regexp_replace, split, col, udf
import hashlib


def run(hive_context, input_table_name, factdata_table_name):
    command = """CREATE TABLE ad_persona_1116 (aid STRING,gender_new_dev STRING,forecast_age_dev STRING,city_v2_dev STRING,price_new_dev STRING,city_new_dev STRING,pt_d STRING) ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde" WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\'")  STORED AS TEXTFILE tblproperties("skip.header.line.count"="1")"""
    hive_context(command)

    command = """LOAD DATA INPATH  "hdfs://fw0016243:8020/user/airflow/{}" INTO TABLE ad_persona_1116""".format(input_table_name)
    hive_context(command)

    # select the data frame and process it
    command = """SELECT * FROM {} where forecast_age_dev != "a.forecast_age_dev" """.format(input_table_name)
    df = hive_context.sql(command)

    command = """CREATE TABLE IF NOT EXISTS {} (aid STRING, gender_new_dev STRING,forecast_age_dev STRING,city_v2_dev STRING,price_new_dev STRING,city_new_dev STRING,pt_d STRING) """.format(factdata_table_name)
    hive_context.sql(command)

    # write the dataframe into the partitioned table
    df.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format('hive').insertInto(factdata_table_name)


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    factdata_table_name = 'ads_persona_11162021'
    input_table_name = "dwd_bigdata_persona.csv"
    run(hive_context=hive_context, input_table_name=input_table_name, factdata_table_name=factdata_table_name)
    sc.stop()
