# Copyright 2020, Futurewei Technologies
#
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

import unittest
from predictor_dl_model.pipeline import main_ts
from pyspark.sql.types import IntegerType
from test_base import TestBase
from data import test_set

# Baohua Cao optimized.


class TestMainTs(TestBase):

    def test_run(self):
        """
        test run() in main_ts.py
        """
        cfg = self.cfg['pipeline']['time_series']
        yesterday = cfg['yesterday']
        prepare_past_days = cfg['prepare_past_days']
        output_table_name = cfg['output_table_name']
        bucket_size = cfg['bucket_size']
        bucket_step = cfg['bucket_step']
        factdata_table_name = cfg['factdata_table_name']
        conditions = cfg['conditions']

        df_tested = self.hive_context.createDataFrame(
            test_set.factdata_main_ts_tested, test_set.factdata_columns)
        df_tested.write.option("header", "true").option(
            "encoding", "UTF-8").mode("overwrite").format(
            'hive').saveAsTable(factdata_table_name)

        main_ts.run(self.hive_context, conditions, factdata_table_name,
                    yesterday, prepare_past_days, output_table_name, 
                    bucket_size, bucket_step)
        command = "select * from {}".format(output_table_name)
        df = self.hive_context.sql(command)

        df_expected = self.hive_context.createDataFrame(
            test_set.factdata_main_ts_expected,
            test_set.factdata_main_ts_columns
        )

        columns = test_set.factdata_main_ts_columns
        self.assertTrue(self.compare_dfs(
            df.select(columns), df_expected.select(columns)))


if __name__ == "__main__":
    unittest.main()
