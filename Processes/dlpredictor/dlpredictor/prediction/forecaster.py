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
import math
import statistics

import dlpredictor.prediction.ims_predictor_util as ims_predictor_util
from dlpredictor.util.sparkesutil import *
from imscommon.model.ucday import UCDay
from imscommon.model.ucday_hourly import UCDay_Hourly
from predictor_dl_model.trainer.client_rest_dl2 import predict


class Forecaster:

    def __init__(self, cfg):
        self.holiday_list = cfg['holiday_list']
        self.cfg = cfg


    def dl_daily_forecast(self, serving_url, model_stats, day_list, ucdoc_attribute_map):
        x,y = predict(serving_url=serving_url, model_stats=model_stats, day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map,forward_offset=0)
        ts = x[0]
        days = y
        return ts, days
