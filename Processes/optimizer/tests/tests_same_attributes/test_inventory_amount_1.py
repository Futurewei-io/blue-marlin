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

# Testing resource inventory amount based on b1, b2, b3 with idnex of bookings_03242020.
# Baohua Cao

import unittest
from imscommon.es.ims_esclient import ESClient
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, ArrayType, MapType, Row
import optimizer.util
import optimizer.main
import json
import os
import warnings

class Unittest_Resource_Inventory_Amount_1(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        with open(fpath + '/data_source/bookings_same_attributes.json') as bookings_source:
            self.bookings = json.load(bookings_source)
        with open(fpath + '/data_source/cfg.json') as cfg_source:
            self.cfg = json.load(cfg_source)
        self.bookings_map = optimizer.util.get_bookings_map(self.bookings)


    def test_resource_inventory_amount_1(self):
        res = dict({'ands': ['b1', 'b2', 'b3'], 'minus': [], 'day': '20180402'})
        resource_inventory_amount = optimizer.main.get_bb_count(cfg, bookings_map)(res['ands'], res['minus'], res['day'])
        self.assertTrue(resource_inventory_amount == 43562)

    def test_resource_inventory_amount_2(self):
        res = dict({'ands': ['b1'], 'minus': ['b2', 'b3'], 'day': '20180402'})
        resource_inventory_amount = optimizer.main.get_bb_count(cfg, bookings_map)(res['ands'], res['minus'], res['day'])
        self.assertTrue(resource_inventory_amount == 0)

    def test_resource_inventory_amount_3(self):
        res = dict({'ands': ['b1', 'b2'], 'minus': ['b3'], 'day': '20180402'})
        resource_inventory_amount = optimizer.main.get_bb_count(cfg, bookings_map)(res['ands'], res['minus'], res['day'])
        self.assertTrue(resource_inventory_amount == 0)

if __name__ == '__main__':
    unittest.main()
