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

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, FloatType, IntegerType, ArrayType, MapType

spark_session = SparkSession.builder \
    .config('hive.metastore.uris', "thrift://10.213.37.46:9083") \
    .config('hive.server2.thrift.http.port', 10002) \
    .config('hive.server2.thrift.port', 10016) \
    .config('hive.server2.transport.mode', 'binary') \
    .config('hive.metastore.client.socket.timeout', 1800) \
    .config('hive.metastore.client.connect.retry.delay', 5) \
    .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict') \
    .enableHiveSupport().getOrCreate()

data = [(1,2), (3,4), (5,6)]

schema = StructType([
    StructField("a", IntegerType(), True),
    StructField("b", IntegerType(), True),
])

df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(data), schema)
df.show()

