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

import unittest, os, yaml
from pyspark import SparkContext
from pyspark.sql import HiveContext


class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load config file
        fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        with open(fpath + '/conf/config.yml', 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)
        cls.cfg = cfg
        sc = SparkContext().getOrCreate()
        sc.setLogLevel('warn')
        cls.hive_context = HiveContext(sc)
