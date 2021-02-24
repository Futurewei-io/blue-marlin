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

# This is a placeholder for a Google-internal import.

import grpc
import numpy as np
import tensorflow as tf
from feeder import VarFeeder

from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2_grpc
from input_pipe import ucdoc_features
from input_pipe import ModelMode
from input_pipe import InputPipe


tf.app.flags.DEFINE_integer('concurrency', 1, 'maximum number of concurrent inference requests')
tf.app.flags.DEFINE_integer('batch_size', 1024, 'number of sample in each batch')
tf.app.flags.DEFINE_integer('predict_window', 10, 'Number of days to predict')
tf.app.flags.DEFINE_integer('train_window', 20, 'number of time spots in training')
tf.app.flags.DEFINE_string('server', '', 'PredictionService host:port')
tf.app.flags.DEFINE_string('result_dir', 'data/predict', 'directory to put prediction result.')
tf.app.flags.DEFINE_boolean('verbose', False, 'verbose or not in creating input data')
FLAGS = tf.app.flags.FLAGS


def main(_):
  if not FLAGS.server:
    print('please specify server host:port')
    return

  channel = grpc.insecure_channel(FLAGS.server)
  stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)
  request = predict_pb2.PredictRequest()

  request.model_spec.name = "ucdoc"
  request.model_spec.signature_name = "serving_default"

  with tf.variable_scope('input') as inp_scope:
    with tf.device("/cpu:0"):
      inp = VarFeeder.read_vars("data/vars")
      pipe = InputPipe(inp, ucdoc_features(inp), inp.n_pages, mode=ModelMode.PREDICT,
                       batch_size=FLAGS.batch_size, n_epoch=1, verbose=FLAGS.verbose,
                       train_completeness_threshold=0.01, predict_window=FLAGS.predict_window,
                       predict_completeness_threshold=0.0, train_window=FLAGS.train_window,
                       back_offset=FLAGS.predict_window+1)
  with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:
      pipe.load_vars(sess)
      pipe.init_iterator(sess)

      while True:
          try:
              truex, timex, normx, laggedx, truey, timey, normy, normmean, normstd, pgfeatures, pageix = \
                  sess.run([pipe.true_x, pipe.time_x, pipe.norm_x, pipe.lagged_x, pipe.true_y, pipe.time_y,
                            pipe.norm_y, pipe.norm_mean, pipe.norm_std, pipe.ucdoc_features, pipe.page_ix])

              request.inputs["truex"].CopyFrom(tf.make_tensor_proto(truex))
              request.inputs["timex"].CopyFrom(tf.make_tensor_proto(timex))
              request.inputs["normx"].CopyFrom(tf.make_tensor_proto(normx))
              request.inputs["laggedx"].CopyFrom(tf.make_tensor_proto(laggedx))
              request.inputs["truey"].CopyFrom(tf.make_tensor_proto(truey))
              request.inputs["timey"].CopyFrom(tf.make_tensor_proto(timey))
              request.inputs["normy"].CopyFrom(tf.make_tensor_proto(normy))
              request.inputs["normmean"].CopyFrom(tf.make_tensor_proto(normmean))
              request.inputs["normstd"].CopyFrom(tf.make_tensor_proto(normstd))
              request.inputs["page_features"].CopyFrom(tf.make_tensor_proto(pgfeatures))
              request.inputs["pageix"].CopyFrom(tf.make_tensor_proto(pageix))

              response = stub.Predict(request, 10)
              tensor_proto = response.outputs['pred']
              if not 'pred_result' in locals():
                  pred_result = tf.contrib.util.make_ndarray(tensor_proto)
              else:
                  pred_result = np.concatenate([pred_result, tf.contrib.util.make_ndarray(tensor_proto)])
          except tf.errors.OutOfRangeError:
              print('done with prediction')
              break
      pred_result = np.expm1(pred_result) + 0.5
      pred_result = pred_result.astype(int)
      if not os.path.exists(FLAGS.result_dir):
          os.mkdir(FLAGS.result_dir)
      result_file = os.path.join(FLAGS.result_dir, "predict.pkl")
      pickle.dump(pred_result, open(result_file, "wb"))
      print('finished prediction')

if __name__ == '__main__':
  #tf.compat.v1.app.run()
  tf.app.run()
