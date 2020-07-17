# Copyright 2019, Futurewei Technologies
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

from collections import namedtuple


class UCHourHistogram:
    # h ranges from h0 to h3,the category of price
    def __init__(self, h, dict):
        self.h = h
        self.t = 0.0    # total_count
        for key, value in dict.items():
            if self.h in key:
                value = UCHourHistogram.satnatize_value(value)
                if 'total_count' in key:
                    self.t = value

    @staticmethod
    def satnatize_value(self, v):
        if v == '' or v == None:
            return '0'
        if type(v) == unicode:
            return v.encode('unicode-escape').decode('string_escape')
        return str(v)

    @staticmethod
    def build(h, dict):
        tmp = namedtuple("UCHourHistogram", dict.keys())(*dict.values())
        r = UCHourHistogram(tmp.h, {})
        r.t = tmp.t
        return r

    @staticmethod
    def add(h, uchour1_h, uchour2_h):
        result = UCHourHistogram(h, {})
        result.t = (uchour1_h.t) + (uchour2_h.t)
        return result

    @staticmethod
    def devide(uchour_h, length_days):
        if length_days == 0:
            return uchour_h
        result = UCHourHistogram(uchour_h.h, {})
        result.t = (float(uchour_h.t) / float(length_days))
        return result
