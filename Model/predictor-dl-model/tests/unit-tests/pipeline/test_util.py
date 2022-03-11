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

# write clusters statistics info to a local .txt for testing the dl model's config parameters.
# use write_clusters_statistics() in the main_cluster.py when needed to print out the statistics.

import unittest
import predictor_dl_model.pipeline.util as model_util


def write_clusters_statistics(df, target, datapoints_threshold_uckeys, datapoints_threshold_clusters,
                              no_of_non_dense_clusters, popularity_norm):

    file_writer = open('./write_clusters_statistics.txt', 'a')
    file_writer.write('\n\n')

    if target == "pre-cluster":
        file_writer.write('write_pre_clusters_statistics: ' +
                          str(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
        file_writer.write('\n\n')
        file_writer.write(
            'datapoints_threshold_uckeys, datapoints_threshold_clusters, no_of_non_dense_clusters, popularity_norm')
        file_writer.write('\n')
        file_writer.write(str(datapoints_threshold_uckeys) + ', ' + str(datapoints_threshold_clusters) + ', ' +
                          str(no_of_non_dense_clusters) + ', ' + str(popularity_norm))
        file_writer.write('\n\n')
        file_writer.write(
            'sparse uckeys count (ratio), dense uckeys count (ratio), total uckeys count')
        file_writer.write('\n')
        df_sparse = df.filter(df.sparse == True)
        df_sparse_count = df_sparse.count()
        df_dense = df.filter(df.sparse == False)
        df_dense_count = df_dense.count()
        df_count = df_sparse_count + df_dense_count
        df_count_str = str(df_sparse_count) + '(%' + \
            "{0:.2f}".format(100.0 * df_sparse_count/df_count) + ')'
        dfd_count_str = str(df_dense_count) + '(%' + \
            "{0:.2f}".format(100.0 * df_dense_count/df_count) + ')'
        file_writer.write(df_count_str + ', ' +
                          dfd_count_str + ', ' + str(df_count))
        file_writer.write('\n\n')
        file_writer.write(
            'sparse uckeys count, no_of_non_dense_clusters, avg uckeys count per cluster')
        file_writer.write('\n')
        sparse_uckeys_per_cluster = int(
            1.0 * df_sparse_count / no_of_non_dense_clusters)
        file_writer.write(str(df_sparse_count) + ', ' +
                          str(no_of_non_dense_clusters) + ', ' + str(sparse_uckeys_per_cluster))
        file_writer.write('\n\n')
        #df_datapoints = df.withColumn("data_points", udf(lambda x: len([xi for xi in x if xi > 0]), IntegerType())(df["ts"]))

    elif target == "cluster":
        file_writer = open('./write_clusters_statistics.txt', 'a')
        file_writer.write('write_clusters_statistics: ' +
                          str(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
        file_writer.write('\n\n')

        df_imp = df.agg({'imp': 'sum'}).withColumnRenamed(
            'sum(imp)', 'sum_imp')
        df_imp_count = df_imp.take(1)[0]['sum_imp']
        dfv = df.filter(df.is_virtual == True)
        dfv_imp = dfv.agg({'imp': 'sum'}).withColumnRenamed(
            'sum(imp)', 'sum_imp')
        dfv_imp_count = dfv_imp.take(1)[0]['sum_imp']
        file_writer.write(
            'virtual clusers count (ratio), dense clusters count (ratio), total clusters count')
        file_writer.write('\n')
        df_count = df.count()
        dfv_count = dfv.count()
        dfd_count = df_count - dfv_count
        df_count_str = str(dfv_count) + '(%' + \
            "{0:.2f}".format(100.0 * dfv_count/df_count) + ')'
        dfd_count_str = str(dfd_count) + '(%' + \
            "{0:.2f}".format(100.0 * dfd_count/df_count) + ')'
        file_writer.write(df_count_str + ', ' +
                          dfd_count_str + ', ' + str(df_count))
        file_writer.write('\n\n')
        file_writer.write(
            'virtual clusers traffic (ratio), dense clusters traffic (ratio), total clusters traffic')
        file_writer.write('\n')
        dfv_imp_count_str = str(
            dfv_imp_count) + '(%' + "{0:.2f}".format(100.0 * dfv_imp_count/df_imp_count) + ')'
        dfd_imp_count = df_imp_count - dfv_imp_count
        dfd_imp_count_str = str(
            dfd_imp_count) + '(%' + "{0:.2f}".format(100.0 * dfd_imp_count/df_imp_count) + ')'
        file_writer.write(dfv_imp_count_str + ', ' +
                          dfd_imp_count_str + ', ' + str(df_imp_count))
        file_writer.write('\n\n')
        file_writer.write(
            '---------------------------------------------------------------------------')
        file_writer.write('\n\n')

    file_writer.close()


class TestUtil(unittest.TestCase):

    def test_resolve_placeholder_1(self):
        cfg = {'key1': 'value1', 'key2': {'key3': 'value2'}}
        model_util.resolve_placeholder(cfg)
        expected = {'key1': 'value1', 'key2': {'key3': 'value2'}}

        self.assertEqual(cfg, expected)

    def test_resolve_placeholder_2(self):
        cfg = {'key1': 'value1', 'key2': {'key3': '{key1}'}}
        model_util.resolve_placeholder(cfg)
        expected = {'key1': 'value1', 'key2': {'key3': 'value1'}}

        self.assertEqual(cfg, expected)

    def test_resolve_placeholder_3(self):
        cfg = {'key1': 'value1', 'key1-1': 'value1-1', 'key2': {'key3': '{key1}-{key1-1}'}}
        model_util.resolve_placeholder(cfg)
        expected = {'key1': 'value1', 'key1-1': 'value1-1','key2': {'key3': 'value1-value1-1'}}

        self.assertEqual(cfg, expected)


if __name__ == '__main__':
    unittest.main()
