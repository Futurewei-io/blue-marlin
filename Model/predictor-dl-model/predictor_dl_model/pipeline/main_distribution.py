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

# accumulation of traffic from sparse uckeys to group uckeys and generate distribution ratios.
import argparse
import yaml

from pyspark.sql import HiveContext
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType
from util import resolve_placeholder


def load_df(hc, table_name):
    command = """select * from {}""".format(table_name)
    return hc.sql(command)


def __calcualte_distrib_ratio(uckey_sum, cluster_uckey_sum):
    if cluster_uckey_sum:
        return 1.0 * uckey_sum / cluster_uckey_sum
    else:
        return 0.0
    # 1.0 * resolved the division issue of python2 to avoid 0 values.

# the major entry function for generating distributions between sparse uckeys and cluster (virtual) uckeys.


def run(df_pre_cluster, df_cluster, table_distrib_output, table_distrib_detail, is_sparse=True):
    """
    pipeline_pre_cluster: (cn is the cluster id, an integer, and it is a string as uckey in df_cluster)
    +--------------------+---------+--------------------+---+---+---+---+---+---+-----------+------------+------+---+
    |               uckey|price_cat|                  ts|  a|  g|  t| si|  r|imp|          p|         p_n|sparse| cn|
    +--------------------+---------+--------------------+---+---+---+---+---+---+-----------+------------+------+---+
    |magazinelock,01,3...|        1|[,,,,,,,,,,1,1,,,...|  3|g_f| 3G| 01|   |  2|0.022222223|-0.106902316|  true|997|
    |magazinelock,01,3...|        1|[,,,,,,,,,,2,8,,,...|  4|g_f| 3G| 01|   |  2|0.022222223|-0.106902316|  true|827|
    +--------------------+---------+--------------------+---+---+---+---+---+---+-----------+------------+------+---+
    pipeline_cluster:
    +-----+---------+--------------------+--------------------+--------------------+--------------------+--------------------+---------+------+---------+
    |uckey|price_cat|                  ts|                   a|                   g|                   t|                  si|        r|   imp|        p|
    +-----+---------+--------------------+--------------------+--------------------+--------------------+--------------------+---------+------+---------+
    | 1037|        1|[9189, 1778, 693,...|[0 -> 0.05263158,...|[ -> 0.078947365,...|[4G -> 0.31578946...|[01 -> 0.02631579...|[ -> 1.0]| 53553| 595.0333|
    | 1039|        1|[2385, 1846, 2182...|[0 -> 0.071428575...|[ -> 0.04761905, ...|[4G -> 0.16666667...|[11 -> 0.02380952...|[ -> 1.0]|153697|1707.7444|
    +-----+---------+--------------------+--------------------+--------------------+--------------------+--------------------+---------+------+---------+
    """
    # filter out the sparse uckeys and make sure type of the "cn" column is string.
    df_pre = df_pre_cluster.filter(col("sparse") == is_sparse).select('uckey', 'price_cat', 'imp', 'cn')
    df_pre = df_pre.withColumn("cn", df_pre["cn"].cast(StringType()))
    df_cluster = df_cluster.withColumnRenamed('uckey', 'cluster_uckey').withColumnRenamed('imp', 'cluster_imp')
    # join the two tables to make all the data ready and remove some repeated columns.
    df_join = df_pre.join(df_cluster, [df_pre.cn == df_cluster.cluster_uckey if is_sparse else
                                       df_pre.uckey == df_cluster.cluster_uckey,
                                       df_pre.price_cat == df_cluster.price_cat],
                          how="inner").drop(df_cluster.price_cat)
    # calculate the distribution ratio with sparse uckey's total imp and the cluster's total imp.
    df_join = df_join.withColumn("ratio", udf(__calcualte_distrib_ratio, FloatType())(df_join.imp, df_join.cluster_imp))
    # output the final result, save the distribtion table and the details table.
    mode = 'overwrite' if is_sparse else 'append'
    df_distrib_output = df_join.select('uckey', 'cluster_uckey', 'price_cat', 'ratio')
    df_distrib_output.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').saveAsTable(table_distrib_output)
    df_distrib_detail = df_join.select('uckey', 'cluster_uckey', 'price_cat', 'ratio', 'imp', 'cluster_imp')
    df_distrib_detail.write.option("header", "true").option("encoding", "UTF-8").mode(mode).format('hive').saveAsTable(table_distrib_detail)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='gen distribution')
    parser.add_argument('config_file')
    args = parser.parse_args()

    # Load config file
    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
        resolve_placeholder(cfg)

    cfg_log = cfg['log']
    cfg = cfg['pipeline']

    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg_log['level'])

    pre_cluster_table_name = cfg['uckey_clustering']['pre_cluster_table_name']
    cluster_table_name = cfg['uckey_clustering']['output_table_name']
    output_table_name = cfg['distribution']['output_table_name']
    output_detail_table_name = cfg['distribution']['output_detail_table_name']

    try:
        # prepare the two required data frames.
        df_pre_cluster = load_df(hive_context, pre_cluster_table_name)
        df_cluster = load_df(hive_context, cluster_table_name)

        run(df_pre_cluster, df_cluster, output_table_name, output_detail_table_name, is_sparse=True)
        # comment out the following line to generate distribution for sparse uckeys only, or execute both.
        run(df_pre_cluster, df_cluster, output_table_name, output_detail_table_name, is_sparse=False)
    finally:
        sc.stop()
