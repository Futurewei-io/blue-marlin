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
from din_model.trainer.rest_client import flatten, str_to_intlist, inputData


class rest_client_test(unittest.TestCase):

    # this function check if the flatten function, which supposed to make one list out of list in the list works correctly or not
    def test_flatten(self):
        lst = [[25], [29, 25], [25], [25], [25], [26, 14, 29], [25, 26, 29, 14], [29, 26, 25, 14],
               [14, 25, 29, 26], [25, 26, 29, 14, 17], [25], [25], [29, 25], [25, 14, 26, 29],
               [26, 25], [25], [25], [14, 7, 25, 26]]
        fl_lst = flatten(lst)

        expected_value = [25, 29, 25, 25, 25, 25, 26, 14, 29, 25, 26, 29, 14, 29, 26, 25, 14,
                          14, 25, 29, 26, 25, 26, 29, 14, 17, 25, 25, 29, 25, 25, 14, 26, 29, 26, 25, 25, 25, 14, 7, 25, 26]

        self.assertEqual(fl_lst, expected_value, "passed")

    # the main function take one list of strings which has the keyword index and the frequency and make a list of keyword for each time interval
    def test_str_to_intlist(self):

        record = ['25:3', '29:6,25:2', '29:1,25:2,14:2', '14:1,29:2,25:2', '29:1', '26:1,14:2,25:4', '14:1,25:3']
        result = str_to_intlist(record)
        expected_value = [[25], [29, 25], [29, 25, 14], [14, 29, 25], [29], [26, 14, 25], [14, 25]]

        self.assertEqual(result, expected_value, "passed")

    # the inputData function take the record as a dictionary and the keyword number that needs prediction plus length of the time interval
    # for the history and return the instance that match the signiture format of the model
    # 2 test cases has been created, one with the longer length of record and one with the shorter length
    def test_inputData(self):
        record = {'ucdoc': 0, 'show_counts': [[25], [29, 25], [29, 25, 14], [14, 29, 25], [29], [26, 14, 25], [14, 25]], 'age': '10', 'gender': '3'}
        keyword = 26
        length = 30
        result = inputData(record, keyword, length)
        expected_value = {'hist_i': [25, 29, 25, 29, 25, 14, 14, 29, 25, 29, 26, 14, 25, 14, 25], 'u': 0, 'i': 26, 'j': 26, 'sl': 15}
        self.assertEqual(result, expected_value, "passed")

    def test_inputData2(self):
        record = {'ucdoc': 0, 'show_counts': [[25], [29, 25], [29, 25, 14], [14, 29, 25], [29], [26, 14, 25], [14, 25]],
                  'age': '10', 'gender': '3'}
        keyword = 26
        length = 3
        result = inputData(record, keyword, length)
        expected_value = {'hist_i': [25, 29, 25, 29, 25, 14], 'u': 0, 'i': 26,
                          'j': 26, 'sl': 6}
        self.assertEqual(result, expected_value, "passed")


if __name__ == '__main__':
    unittest.main()
