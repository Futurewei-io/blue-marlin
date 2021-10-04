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

pyspark --executor-memory 16G --driver-memory 24G --num-executors 16 --executor-cores 5 --master yarn --conf hive.exec.max.dynamic.partitions=1024 --conf spark.driver.maxResultSize=8g

'''


from pyspark.sql.types import IntegerType, ArrayType, StringType
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import when, regexp_replace, split,col, udf
import hashlib

def assign_new_bucket_id(df, n):
    def __hash_sha256(s):
        hex_value = hashlib.sha256(s.encode('utf-8')).hexdigest()
        return int(hex_value, 16)
    _udf = udf(lambda x: __hash_sha256(x) % n)
    df = df.withColumnRenamed('bucket_id', 'old_bkid')
    df = df.withColumn('bucket_id', _udf(df.uckey))
    return df

def run(hive_context, file_name):
    new_bucket_size = 900
    # hive_context.sql(command)
    table_name = file_name.replace(".txt", "").replace(".","_")
    ## Create the table without any partition
    command = """
            CREATE TABLE  {} (uckey STRING, count_array array<string>,hour INT, day STRING, bucket_id INT) ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde" WITH SERDEPROPERTIES ( "separatorChar" = ",", "quoteChar" = "\'")  STORED AS TEXTFILE tblproperties("skip.header.line.count"="1") 
            """.format(table_name)
    hive_context.sql(command)


    ## Load the data one by one into the table
    command = """
        LOAD DATA INPATH  "hdfs://fw0016243:8020/user/airflow/{}" INTO TABLE {}
        """.format(file_name, table_name)
    hive_context.sql(command)
       
    ## select the data frame and process it
    command = """select * from {}""".format(table_name)
    df = hive_context.sql(command)

    df = df.withColumn("bucket_id", df["bucket_id"].cast(IntegerType()))
    df = df.withColumn("hour", df["hour"].cast(IntegerType()))
    df = df.withColumn("count_array", when(df.count_array.endswith("]"), regexp_replace(df.count_array, "\]", "")))
    df = df.withColumn("count_array", when(df.count_array.startswith("["), regexp_replace(df.count_array, "\[", "")))
    df = df.withColumn("count_array", regexp_replace(df.count_array, '\"', ''))
    df = df.withColumn("count_array", split(col("count_array"), ",").cast(ArrayType(StringType())))
    df = df.filter("count_array IS NOT NULL")
    df = assign_new_bucket_id(df, new_bucket_size)
    df = df.drop('old_bkid')
    df = df.withColumn("bucket_id", df["bucket_id"].cast(IntegerType()))
    ### create an empty partitioned table
    command = """CREATE TABLE IF NOT EXISTS factdata_09202021 (uckey STRING, count_array array<string>, hour INT, day STRING) partitioned by ( bucket_id INT)"""
    hive_context.sql(command)

    ### write the dataframe into the partitioned table
    df.write.option("header", "true").option("encoding", "UTF-8").mode("append").format('hive').insertInto("factdata_09202021")


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    hive_context.setConf("hive.exec.dynamic.partition", "true")
    hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hive_context.sql('SET spark.hadoop.hive.exec.max.dynamic.partitions=1024 ')
    file_name = "adhoctemp.tmp_z00380608_20210731_FACTDATA_DM_10.txt"
    run(hive_context, file_name=file_name)
    sc.stop()