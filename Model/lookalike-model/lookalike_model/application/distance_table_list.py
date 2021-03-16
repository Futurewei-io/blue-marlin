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
import yaml
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, col, udf, array, mean
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType
import argparse
from pyspark.sql.functions import udf
import time


def dot(l1):
    def _dot(l2):
        list = []
        for item in l1:
            similarity = sum([item[i]*l2[i] for i in range(len(item))])
            list.append(similarity)
        return list
    return _dot



def ux(l1):
    _udf_similarity = udf(dot(l1), ArrayType(FloatType()) )
    return _udf_similarity



def l(d):
    s = [value for key, value in d.items()]
    return s
udf_tolist = udf(l, ArrayType(FloatType()))

def top_n(l):
    #### top 10
    n = 10
    l.sort()
    return l[-n:]
udf_top_n = udf(top_n, ArrayType(FloatType()))

def _top_n(l1, l2):
    n = 10
    l = sorted(l1+l2)
    return l[-n:]

_udf_top_n = udf(_top_n, ArrayType(FloatType()))

def _mean(l):
    ave = sum(l)/len(l)
    return ave
udf_mean = udf(_mean, FloatType())

def run(hive_context, cfg):
    # load dataframes
    lookalike_score_table_norm = cfg['output']['did_score_table_norm']
    keywords_table = cfg["input"]["keywords_table"]
    seeduser_table = cfg["input"]["seeduser_table"]
    lookalike_similarity_table = cfg["output"]["similarity_table"]

    command = "SELECT * FROM {}"
    df = hive_context.sql(command.format(lookalike_score_table_norm))
    df_keywords = hive_context.sql(command.format(keywords_table))
    df_seed_user = hive_context.sql(command.format(seeduser_table))


    #### creating a tuple of did and kws for seed users
    df = df.withColumn('kws_norm_list', udf_tolist(col('kws_norm')))
    df_seed_user = df_seed_user.join(df.select('did','kws_norm_list'), on=['did'], how='left')
    seed_user_list = df_seed_user.select('did', 'kws_norm_list').collect()

## batch 1 : 0-100 801 seed
    batch_length = 800
    c = 0
    #### i=0, c=0 , batched_user=[0,200], top_10
    total_c = len(seed_user_list)
    df = df.withColumn('top_10', array(lit(0.0)))
    while total_c > 0 :
            len_tobe_p = min(batch_length,total_c)
            total_c-= len_tobe_p
            batched_user = [item[1] for item in seed_user_list[c: c+len_tobe_p]]
            df = df.withColumn("similarity_list",ux(batched_user)(col('kws_norm_list')))
            df = df.withColumn("top_10", _udf_top_n(col("similarity_list"),col("top_10")))
            c+=len_tobe_p

    df = df.withColumn("mean_score",udf_mean(col("top_10")))
    df.write.option("header", "true").option(
        "encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(lookalike_similarity_table)
    extended_did = df.sort(col('mean_score').desc()).select('did', 'mean_score')



if __name__ == "__main__":
    start = time.time()
    parser = argparse.ArgumentParser(description=" ")
    parser.add_argument('config_file')
    args = parser.parse_args()
    with open(args.config_file, 'r') as yml_file:
        cfg = yaml.safe_load(yml_file)

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
    end = time.time()
    print('Runtime of the program is:', (end - start))
