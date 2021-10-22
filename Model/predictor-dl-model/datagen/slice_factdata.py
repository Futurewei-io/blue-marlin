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

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType
from datetime import datetime, timedelta

# scp slice_factdata.py saya@10.213.37.40:~/code/bluemarlin-models/predictor_dl_model/predictor_dl_model/datagen/

# happened to have
# 2230 uckeys
# one of the uckeys native,72bcd2720e5011e79bc8fa163e05184e,WIFI,g_m,5,CPM,15,76
# Note: this uckey has only 18 hours, the rest of the hours are ), we checked and this uckey is not assgined to any other bucket_id

sc = SparkContext()
hive_context = HiveContext(sc)

table_name = 'factdata_reza_06222020'

command = """
        DROP TABLE IF EXISTS {}
        """.format(table_name)
hive_context.sql(command)

command = """CREATE TABLE IF NOT EXISTS {}(uckey string,bucket_id int,count_array array<string>,hour int, day string)""".format(
    table_name)
hive_context.sql(command)

df=hive_context.sql("select * from modified_ready_factdata_06162020 where day='2019-11-02'or day='2019-11-03' and bucket_id between 1 and 2")

df.select('uckey','bucket_id','count_array','hour','day').write.format('hive').option("header", "true").option("encoding", "UTF-8").mode('append').insertInto(table_name)


