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

"""
A client that talks to tensorflow_serving loaded with kaggle model.

The client read kaggle feature data set, queries the service with
such feature data to get predictions, and calculates the inference error rate.

Typical usage example:

    predictor_client.py --server=0.0.0.0:8500
"""

from __future__ import print_function

import sys
import threading
import os
import pickle
import tensorflow as tf
from predictor_dl_model.trainer.feeder import VarFeeder
from predictor_dl_model.trainer.input_pipe import ucdoc_features
from predictor_dl_model.trainer.input_pipe import ModelMode
from predictor_dl_model.trainer.input_pipe import InputPipe
from enum import Enum
from typing import List, Iterable
import numpy as np
import pandas as pd
import json
import requests



tf.app.flags.DEFINE_integer(
    'concurrency', 1, 'maximum number of concurrent inference requests')
tf.app.flags.DEFINE_integer(
    'batch_size', 1024, 'number of sample in each batch')
tf.app.flags.DEFINE_integer('predict_window', 10, 'Number of days to predict')
tf.app.flags.DEFINE_integer(
    'train_window', 60, 'number of time spots in training')
tf.app.flags.DEFINE_string('server', '', 'PredictionService host:port')
tf.app.flags.DEFINE_string(
    'result_dir', 'data/predict', 'directory to put prediction result.')
tf.app.flags.DEFINE_boolean(
    'verbose', False, 'verbose or not in creating input data')
FLAGS = tf.app.flags.FLAGS

BATCH_SIZE = 1 # Has to be 1
def main(_):
    # if not FLAGS.server:
    #     print('please specify server host:port')
    #     return

    with tf.variable_scope('input') as inp_scope:
        with tf.device("/cpu:0"):
            inp = VarFeeder.read_vars("data/vars")
            pipe = InputPipe(inp, ucdoc_features(inp), inp.hits.shape[0], mode=ModelMode.PREDICT,
                             batch_size=BATCH_SIZE, n_epoch=1, verbose=False,
                             train_completeness_threshold=0.01, predict_window=10,
                             predict_completeness_threshold=0.0, train_window=60,
                             back_offset=11)
    error = []
    with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:


        pipe.load_vars(sess)
        pipe.init_iterator(sess)
        for i in range(100):

            truex, timex, normx, laggedx, truey, timey, normy, normmean, normstd, pgfeatures, pageix = sess.run([pipe.true_x, pipe.time_x, pipe.norm_x, pipe.lagged_x, pipe.true_y, pipe.time_y,
                                                                                                                 pipe.norm_y, pipe.norm_mean, pipe.norm_std, pipe.ucdoc_features, pipe.page_ix])
            # if pageix == b'cloudFolder,2,4G,g_f,2,pt,1002,icc,3,10':
            #     print("hello")


            data = {"instances": [{"truex": truex.tolist()[0], "timex": timex.tolist()[0], "normx": normx.tolist()[0], "laggedx": laggedx.tolist()[0],
                                   "truey": truey.tolist()[0], "timey": timey.tolist()[0], "normy": normy.tolist()[0], "normmean": normmean.tolist()[0],
                                   "normstd": normstd.tolist()[0], "page_features": pgfeatures.tolist()[0], "pageix": [pageix.tolist()[0].decode('utf-8')]}]}

            URL = "http://10.193.217.108:8501/v1/models/faezeh1:predict"
            body = data
            r = requests.post(URL, data=json.dumps(body))
            pred_y=np.round(np.expm1(r.json()['predictions'][0]))
            true_y = np.round(np.expm1(truey))
            e = np.average(np.divide(np.abs(np.subtract(pred_y,true_y)),true_y))
            error.append(e)
            print( data['instances'][0]['pageix'][0], e  )
        print(np.average(error))




if __name__ == '__main__':
    tf.app.run()
