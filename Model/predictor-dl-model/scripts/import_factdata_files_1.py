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
This file process raw tables. Raw tables are created by import_factdata_files.py

spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=5G --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict import_factdata_files_1.py

'''


from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import when, regexp_replace, split, col, udf
import hashlib


def run(hive_context, input_table_name, factdata_table_name):

    # select the data frame and process it
    command = """SELECT * FROM {}""".format(input_table_name)
    df = hive_context.sql(command)

    df = df.withColumn("bucket_id", df["bucket_id"].cast(IntegerType()))
    df = df.withColumn("hour", df["hour"].cast(IntegerType()))
    df = df.withColumn("count_array", when(df.count_array.endswith("]"), regexp_replace(df.count_array, "\]", "")))
    df = df.withColumn("count_array", when(df.count_array.startswith("["), regexp_replace(df.count_array, "\[", "")))
    df = df.withColumn("count_array", regexp_replace(df.count_array, '\"', ''))
    df = df.withColumn("count_array", split(col("count_array"), ",").cast(ArrayType(StringType())))
    df = df.filter("count_array IS NOT NULL")

    command = """CREATE TABLE IF NOT EXISTS {} (uckey STRING, count_array array<string>, hour INT, day STRING) PARTITIONED BY (bucket_id INT)""".format(factdata_table_name)
    hive_context.sql(command)

    # write the dataframe into the partitioned table
    df.write.option("header", "true").option("encoding", "UTF-8").mode("append").format('hive').insertInto(factdata_table_name)


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    factdata_table_name = 'factdata_10182021'
    input_table_name = "adhoctemp_tmp_z00380608_20210731_factdata_dm_01"
    run(hive_context=hive_context, input_table_name=input_table_name, factdata_table_name=factdata_table_name)
    sc.stop()
