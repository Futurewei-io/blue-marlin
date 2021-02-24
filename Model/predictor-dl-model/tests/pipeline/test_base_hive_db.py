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
import multiprocessing
import yaml, threading

from pyspark import SparkContext
from pyspark.sql import HiveContext

def load_df(hive_context, table_name, bucket_id):
    command = """select * from {} where bucket_id = {}""".format(table_name, bucket_id)
    return hive_context.sql(command)

class TestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open('config.yml', 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        cls.cfg = cfg

        sc = SparkContext().getOrCreate()
        sc.setLogLevel('warn')
        cls.hive_context = HiveContext(sc)
        cls.table_name = cfg['test']['table_name']
        cls.df = load_df(hive_context=cls.hive_context, table_name=cls.table_name, bucket_id=1)
        cls.df2 = load_df(hive_context=cls.hive_context, table_name=cls.table_name, bucket_id=2)
        cls.timeout = cfg['test']['timer']
        cls.manager = multiprocessing.Manager()
        cls.return_dic = cls.manager.dict()

    def timer(self, timeout, func, args=(), kwargs={}):
        """ Run func with the given timeout.
            If func didn't finish running within the timeout, return -1.
        """

        class UnitTestFuncThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)
                self.result = None
                self._stop_event = threading.Event()

            def run(self):
                self.result = func(*args, **kwargs)

            def stop(self):
                self._stop_event.set()

            def stopped(self):
                return self._stop_event.is_set()

        func_thread = UnitTestFuncThread()
        func_thread.daemon = True
        func_thread.start()
        func_thread.join(timeout)
        if func_thread.isAlive():
            func_thread.stop()
            return -1  # -1: the outtime failure with failed message.
        else:
            return 0

