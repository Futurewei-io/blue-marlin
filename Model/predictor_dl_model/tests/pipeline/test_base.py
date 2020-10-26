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
import yaml
from pyspark import SparkContext
from pyspark.sql import HiveContext


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open('config.yml', 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        cls.cfg = cfg
        sc = SparkContext().getOrCreate()
        sc.setLogLevel('warn')
        cls.hive_context = HiveContext(sc)
        cls.timeout = cfg['test']['timer']

    def compare_dfs(self, df1, df2):
        col1 = sorted(df1.collect())
        col2 = sorted(df2.collect())
        return col1 == col2
