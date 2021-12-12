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
This file load data into a temperary table and create a showlog table out of it

spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict import_factdata_files_1.py

'''


from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import when, regexp_replace, split, col, udf
import hashlib


def run(hive_context, input_table_names, log_table_name):

    for input_table_name in input_table_names:
        command = """ CREATE TABLE ad_show_1116 (logid STRING,result_type STRING,contend_id STRING,advertiser_id STRING,order_id STRING,task_id STRING,price_type STRING,creativetype STRING,ad_type STRING,app_name STRING,device_type STRING,slot_id STRING,device_size STRING,network_type STRING,showid STRING,event_time STRING,event_type STRING,promote_app_id STRING,industry_id STRING,isvalid STRING,industry_type STRING,did STRING,aid STRING,pt_h STRING) ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde" WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\'")  STORED AS TEXTFILE tblproperties("skip.header.line.count"="1") """
        hive_context(command)
        command = """LOAD DATA INPATH  "hdfs://fw0016243:8020/user/airflow/{}" INTO TABLE ad_click_1116""".format(input_table_name,)
        hive_context(command)
        command = """SELECT * FROM ad_show_1116 WHERE price_type != "a.price_type" """
        df = hive_context(command)
        command = """CREATE TABLE IF NOT EXISTS {} (logid STRING,result_type STRING,contend_id STRING,advertiser_id STRING,order_id STRING,task_id STRING,price_type STRING,creativetype STRING,ad_type STRING,app_name STRING,device_type STRING,slot_id STRING,device_size STRING,network_type STRING,showid STRING,event_time STRING,event_type STRING,promote_app_id STRING,industry_id STRING,isvalid STRING,industry_type STRING,did STRING,aid STRING,pt_h STRING) """.format(log_table_name)
        hive_context(command)
        df.write.option("header", "true").option("encoding", "UTF-8").mode("append").format('hive').insertInto(log_table_name)



if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    log_table_name = 'ads_showlog_11162021'
    input_table_names = ["dwd_pps_imp_cdr_hm_splitaa", "dwd_pps_imp_cdr_hm_splitab", "dwd_pps_imp_cdr_hm_splitac", "dwd_pps_imp_cdr_hm_splitad", "dwd_pps_imp_cdr_hm_splitae"]
    run(hive_context=hive_context, input_table_names=input_table_names, log_table_name=log_table_name)
    sc.stop()
