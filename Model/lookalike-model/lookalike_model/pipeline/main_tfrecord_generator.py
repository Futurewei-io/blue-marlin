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

"""
spark-submit --master yarn --executor-memory 16G --driver-memory 24G --num-executors 10 --executor-cores 5 --jars spark-tensorflow-connector_2.11-1.15.0.jar --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/main_tfrecord_generator.py config.yml
input: trainready table
output: dataset readable by trainer in tfrecord format
"""

from pyspark.sql.functions import lit, udf, explode
from pyspark.sql.types import IntegerType, ArrayType, StructType, StructField
from util import save_pickle_file, resolve_placeholder, load_config


def generate_tfrecord(hive_context, tf_stat_path, keyword_table, cutting_date, length, trainready_table,
                      tfrecords_hdfs_path_train, tfrecords_hdfs_path_test, aid_bucket_size, search_bucket_step):

    def str_to_intlist(table):
        ji = []
        for k in [table[j].decode().split(",") for j in range(len(table))]:
            s = []
            for a in k:
                b = int(a.split(":")[1])
                s.append(b)
            ji.append(s)
        return ji

    def list_of_list_toint(table):
        ji = []
        for k in [table[j].decode().split(",") for j in range(len(table))]:
            s = [int(a) for a in k]
            ji.append(s)
        return ji

    def padding(kwlist,length):
        diff = length-len(kwlist)
        temp_list = [0 for i in range(diff)]
        padded_keyword = kwlist + temp_list
        return padded_keyword

    def generating_dataframe(df, current_max_length ):
        df = df.withColumn('max_length', lit(current_max_length))
        df = df.withColumn('keyword_list_padded',
                                     udf(padding, ArrayType(IntegerType()))(df.keyword_list, df.max_length))
        return df

    def generate_tf_statistics(testsetDF, trainDF, keyword_df, tf_stat_path):
        tfrecords_statistics = {}
        tfrecords_statistics['test_dataset_count'] = testsetDF.count()
        tfrecords_statistics['train_dataset_count'] = trainDF.count()
        tfrecords_statistics['user_count'] = trainDF.select('aid').distinct().count()
        tfrecords_statistics['item_count'] = keyword_df.distinct().count() + 1
        save_pickle_file(tfrecords_statistics, tf_stat_path)

    def create_trainset(aid_index, click_counts, keyword_int):
        def flatten(lst):
            f = [y for x in lst for y in x]
            return f
        t_set = []
        for m in range(len(click_counts)):
            for n in range(len(click_counts[m])):
                if (click_counts[m][n] != 0):
                    pos = (aid_index, flatten(keyword_int[m + 1:m + 1 + length]), keyword_int[m][n], 1)
                    if len(pos[1]) >= 1:
                        t_set.append(pos)
                elif (m % 5 == 0 and n % 2 == 0):
                    neg = (aid_index, flatten(keyword_int[m + 1:m + 1 + length]), keyword_int[m][n], 0)
                    if len(neg[1]) >= 1:
                        t_set.append(neg)
        return t_set

    schema = StructType([
        StructField("aid_index", IntegerType(), True),
        StructField("keyword_list", ArrayType(IntegerType()), True),
        StructField("keyword", IntegerType(), True),
        StructField("label", IntegerType(), True)
        ])

    round_batch =0
    tfrecords_hdfs_path_train_temp = tfrecords_hdfs_path_train + "_temp"
    tfrecords_hdfs_path_test_temp = tfrecords_hdfs_path_test + "_temp"

    for aid_bucket in range(0, aid_bucket_size, search_bucket_step):
        print('Searching alpha buckets {} - {} of {}'.format(aid_bucket, aid_bucket+search_bucket_step-1, aid_bucket_size))
        command = """SELECT * FROM {} where aid_bucket = {}"""
        df = hive_context.sql(command.format(trainready_table, aid_bucket ))

        df = df.withColumn('interval_starting_time', df['interval_starting_time'].cast(ArrayType(IntegerType())))
        df = df.withColumn('_kwi', udf(list_of_list_toint, ArrayType(ArrayType(IntegerType())))(df.kwi))
        df = df.withColumn('click_counts', udf(str_to_intlist, ArrayType(ArrayType(IntegerType())))(df['kwi_click_counts']))
        df = df.withColumn('total_click', udf(lambda x: sum([item for sublist in x for item in sublist]), IntegerType())(df.click_counts))
        df = df.where(df.total_click != 0)
        df = df.withColumn('indicing', udf(lambda y: len([x for x in y if x >= cutting_date]), IntegerType())(df.interval_starting_time))
        df = df.withColumn('keyword_int_train', udf(lambda x, y: x[y:],ArrayType(ArrayType(IntegerType())))(df._kwi, df.indicing))
        df = df.withColumn('keyword_int_test', udf(lambda x, y: x[:y],ArrayType(ArrayType(IntegerType())))(df._kwi, df.indicing))
        df = df.withColumn('click_counts_train', udf(lambda x, y: x[y:],ArrayType(ArrayType(IntegerType())))(df.click_counts, df.indicing))
        df = df.withColumn('click_counts_test', udf(lambda x, y: x[:y],ArrayType(ArrayType(IntegerType())))(df.click_counts, df.indicing))
        df = df.withColumn('train_set', udf(create_trainset, ArrayType(schema))(df.aid_index, df.click_counts_train,df.keyword_int_train))
        df = df.withColumn('test_set', udf(create_trainset, ArrayType(schema))(df.aid_index, df.click_counts_test, df.keyword_int_test))

        trainDF = df.select(df.aid_index, explode(df.train_set).alias('dataset'))
        testDF  = df.select(df.aid_index, explode(df.test_set).alias('dataset'))
        trainDF = trainDF.select('aid_index', trainDF.dataset['aid_index'].alias('aid'),
                                   trainDF.dataset['keyword_list'].alias('keyword_list'),
                                   trainDF.dataset['keyword'].alias('keyword'), trainDF.dataset['label'].alias('label'))
        testDF = testDF.select('aid_index', testDF.dataset['aid_index'].alias('aid'),
                                 testDF.dataset['keyword_list'].alias('keyword_list'),
                                 testDF.dataset['keyword'].alias('keyword'), testDF.dataset['label'].alias('label'))

        trainDF = trainDF.withColumn('sl', udf(lambda x: len(x), IntegerType())(trainDF.keyword_list))
        testDF = testDF.withColumn('sl', udf(lambda x: len(x), IntegerType())(testDF.keyword_list))
        train_set = trainDF.where(trainDF.sl > 5)
        test_set = testDF.where(testDF.sl > 5)
        train_set = train_set.withColumn('aid_bucket', lit(aid_bucket))
        test_set = test_set.withColumn('aid_bucket', lit(aid_bucket))


        if round_batch ==0:
            train_set.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(tfrecords_hdfs_path_train_temp)
            test_set.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format(
                'hive').saveAsTable(tfrecords_hdfs_path_test_temp)
            round_batch = 1
        else:
            train_set.write.option("header", "true").option("encoding", "UTF-8").mode("append").format(
                'hive').insertInto(tfrecords_hdfs_path_train_temp)
            test_set.write.option("header", "true").option("encoding", "UTF-8").mode("append").format(
                'hive').saveAsTable(tfrecords_hdfs_path_test_temp)

    current_max_length = 0
    for aid_bucket in range(0, aid_bucket_size, search_bucket_step):
        print('Finding the max length of keyword list {} - {} of {}'.format(aid_bucket, aid_bucket + search_bucket_step - 1,
                                                             aid_bucket_size))
        command = """SELECT * FROM {} where aid_bucket = {}"""
        df = hive_context.sql(command.format(tfrecords_hdfs_path_test_temp, aid_bucket))
        max_length1 = df.agg({'sl': 'max'}).collect()[0][0]
        df = hive_context.sql(command.format(tfrecords_hdfs_path_train_temp, aid_bucket))
        max_length2 = df.agg({'sl': 'max'}).collect()[0][0]
        if max_length1 > current_max_length:
            current_max_length = max_length1
        if max_length2 > current_max_length:
            current_max_length = max_length2


    round_batch =0
    for aid_bucket in range(0, aid_bucket_size, search_bucket_step):
        print('Searching alpha buckets {} - {} of {}'.format(aid_bucket, aid_bucket + search_bucket_step - 1,
                                                             aid_bucket_size))
        command = """SELECT * FROM {} where aid_bucket = {}"""
        df_test = hive_context.sql(command.format(tfrecords_hdfs_path_test_temp, aid_bucket))
        df_train = hive_context.sql(command.format(tfrecords_hdfs_path_train_temp, aid_bucket))
        train_set = generating_dataframe(df_train, current_max_length)
        test_set = generating_dataframe(df_test, current_max_length)

        if round_batch ==0:
            train_set.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable(tfrecords_hdfs_path_train)
            test_set.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format(
                'hive').saveAsTable(tfrecords_hdfs_path_test)
            round_batch = 1
        else:
            train_set.write.option("header", "true").option("encoding", "UTF-8").mode("append").format(
                'hive').insertInto(tfrecords_hdfs_path_train)
            test_set.write.option("header", "true").option("encoding", "UTF-8").mode("append").format(
                'hive').saveAsTable(tfrecords_hdfs_path_test)

    command = """DROP TABLE {}"""
    hive_context.sql(command.format(tfrecords_hdfs_path_test_temp))
    hive_context.sql(command.format(tfrecords_hdfs_path_train_temp))

    ######## Read the data from hive and write it to tfrecords
    command = """SELECT * FROM {}"""
    testsetDF = hive_context.sql(command.format(tfrecords_hdfs_path_test))
    train_set = hive_context.sql(command.format(tfrecords_hdfs_path_train))
    train_set.write.format("tfrecords").option("recordType", "Example").mode("overwrite").save(tfrecords_hdfs_path_train)
    testsetDF.write.format("tfrecords").option("recordType", "Example").mode("overwrite").save(tfrecords_hdfs_path_test)

    ######## Create a statistics
    command = """SELECT * from {}"""
    keyword_df = hive_context.sql(command.format(keyword_table))
    generate_tf_statistics(testsetDF, train_set, keyword_df, tf_stat_path)

def run(hive_context, cfg):
    cfgp = cfg['pipeline']
    cfg_train = cfg['pipeline']['main_trainready']
    trainready_table = cfg_train['trainready_output_table']
    cfg_tfrecord = cfg['pipeline']['tfrecords']
    tfrecords_hdfs_path_train = cfg_tfrecord['tfrecords_hdfs_path_train']
    tfrecords_hdfs_path_test = cfg_tfrecord['tfrecords_hdfs_path_test']
    cutting_date = cfg['pipeline']['cutting_date']
    length = cfg['pipeline']['length']
    tf_stat_path = cfgp['tfrecords']['tfrecords_statistics_path']
    keyword_table = cfgp['main_keywords']['keyword_output_table']
    search_bucket_step = cfgp['main_clean']['search_bucket_step']
    aid_bucket_size = cfgp['main_clean']['did_bucket_num']


    generate_tfrecord(hive_context, tf_stat_path, keyword_table, cutting_date, length, trainready_table, tfrecords_hdfs_path_train,
                      tfrecords_hdfs_path_test, aid_bucket_size, search_bucket_step )


if __name__ == "__main__":
    """
    This program performs the followings:
    adds normalized data by adding index of features
    groups data into time_intervals and dids (labeled by did)
    """
    sc, hive_context, cfg = load_config(description="pre-processing train ready data")
    resolve_placeholder(cfg)
    run(hive_context=hive_context, cfg=cfg)
    sc.stop()
