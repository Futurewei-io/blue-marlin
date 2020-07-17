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

import datetime

from imscommon.model.uchourhistogram import UCHourHistogram


class UCHour:

    def __init__(self, h_index):
        # self.h_index = h_index
        # h0 to h3 means price category
        self.h0 = 0
        self.h1 = 0
        self.h2 = 0
        self.h3 = 0
        # this is the total value of h0-h3
        self.total = 0

    def load(self, df_row):
        dict = df_row.asDict()
        # self.h_index = UCHour.extract_h_index(df_row)
        self.h0 = UCHourHistogram('h0', dict).t
        self.h1 = UCHourHistogram('h1', dict).t
        self.h2 = UCHourHistogram('h2', dict).t
        self.h3 = UCHourHistogram('h3', dict).t
        # ht is double sum of h0-h3
        self.total = self.h0 + self.h1 + self.h2 + self.h3

    def histogram_value(self, i):
        if i == 0:
            return self.h0
        elif i == 1:
            return self.h1
        elif i == 2:
            return self.h2
        else:
            return self.h3

    @staticmethod
    def extract_h_index(df_row):
        date = datetime.datetime.strptime(
            df_row.show_time, "%Y-%m-%d %H:%M:%S.%f")
        return date.hour

    @staticmethod
    def buildFromDataFrameRow(daraFrameRow):
        uchour = UCHour(0)  # initialize an UCHour object and h_index
        uchour.load(daraFrameRow)
        return uchour

    @staticmethod
    def buildv1(_dict):
        uchour = UCHour(0)
        uchour.h0 = _dict['h0']['t']
        uchour.h1 = _dict['h1']['t']
        uchour.h2 = _dict['h2']['t']
        uchour.h3 = _dict['h3']['t']
        uchour.total = uchour.h0 + uchour.h1 + uchour.h2 + uchour.h3
        return uchour

    @staticmethod
    def buildv2(_dict):
        uchour = UCHour(0)
        uchour.h0 = _dict['h0']
        uchour.h1 = _dict['h1']
        uchour.h2 = _dict['h2']
        uchour.h3 = _dict['h3']
        uchour.total = uchour.h0 + uchour.h1 + uchour.h2 + uchour.h3
        return uchour

    @staticmethod
    def add(uchour1, uchour2):
        result = UCHour(0)
        result.h0 = uchour1.h0 + uchour2.h0
        result.h1 = uchour1.h1 + uchour2.h1
        result.h2 = uchour1.h2 + uchour2.h2
        result.h3 = uchour1.h3 + uchour2.h3
        result.total = uchour1.total + uchour2.total
        return result

    @staticmethod
    def safe_devide(n1, n2):
        if abs(n2-n1) < 0.001:
            return 1
        return float(n1) / float(n2)

    @staticmethod
    def devide_uchours(uchour1, uchour2):
        result = UCHour(0)
        result.h0 = UCHour.safe_devide(uchour1.h0, uchour2.h0)
        result.h1 = UCHour.safe_devide(uchour1.h1, uchour2.h1)
        result.h2 = UCHour.safe_devide(uchour1.h2, uchour2.h2)
        result.h3 = UCHour.safe_devide(uchour1.h3, uchour2.h3)
        result.total = UCHour.safe_devide(uchour1.total, uchour2.total)
        return result

    @staticmethod
    def multiply_uchours(uchour1, uchour2):
        result = UCHour(0)
        result.h0 = uchour1.h0 * uchour2.h0
        result.h1 = uchour1.h1 * uchour2.h1
        result.h2 = uchour1.h2 * uchour2.h2
        result.h3 = uchour1.h3 * uchour2.h3
        result.total = uchour1.total * uchour2.total
        return result

    @staticmethod
    def devide(uchour, length_days):
        result = UCHour(0)
        result.h0 = float(uchour.h0) / length_days
        result.h1 = float(uchour.h1) / length_days
        result.h2 = float(uchour.h2) / length_days
        result.h3 = float(uchour.h3) / length_days
        result.total = float(uchour.total) / length_days
        return result

    @staticmethod
    def multiply(uchour, factor):
        result = UCHour(0)
        result.h0 = float(uchour.h0) * factor
        result.h1 = float(uchour.h1) * factor
        result.h2 = float(uchour.h2) * factor
        result.h3 = float(uchour.h3) * factor
        result.total = float(uchour.total) * factor
        return result

