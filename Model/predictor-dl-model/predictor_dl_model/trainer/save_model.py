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

#! /usr/bin/env python
"""generate tensorflow serving servable model from trained checkpoint model.

The program uses information from input data and trained checkpoint model
to generate TensorFlow SavedModel that can be loaded by standard
tensorflow_model_server.

sample command:
    python save_model.py --data_dir=data/vars --ckpt_dir=data/cpt/s32 --saved_dir=/tmp/tfserving/serving/servable/ucdoc --model_version=1
"""

from __future__ import print_function

import os
import sys

# This is a placeholder for a Google-internal import.

import tensorflow as tf
import shutil
from feeder import VarFeeder
from input_pipe import InputPipe, ModelMode, ucdoc_features
from hparams import build_from_set, build_hparams
from model import Model
from collections import Iterable
import yaml
import math
import pandas as pd
from datetime import date, datetime, timedelta
import numpy as np

tf.app.flags.DEFINE_integer('training_iteration', 1000, 'number of training iterations.')
tf.app.flags.DEFINE_string('hparam_set', default='s32', help="Hyperparameters set to use (see hparams.py for available sets)")
tf.app.flags.DEFINE_integer('batch_size', 1024, 'number of sample in each batch')
tf.app.flags.DEFINE_integer('model_version', 1, 'version number of the model.')
tf.app.flags.DEFINE_integer('target_model', 1, 'the index of model to use.')
tf.app.flags.DEFINE_integer('n_models', 1, 'number of the model.')
tf.app.flags.DEFINE_integer('train_window', 80, 'number of time spots in training')
tf.app.flags.DEFINE_integer('predict_window', 20, 'Number of days to predict')
tf.app.flags.DEFINE_integer('back_offset', 0, 'Number of days not to use')
tf.app.flags.DEFINE_string('config_file', '../config.yml', 'pipeline configuration file.')
tf.app.flags.DEFINE_string('saved_dir', '/tmp/faezeh', 'directory to save generated tfserving model.')
tf.app.flags.DEFINE_string('ckpt_dir', default='data/cpt/s32', help='checkpint directory')
tf.app.flags.DEFINE_string('data_dir', 'data/vars', 'input data directory')
tf.app.flags.DEFINE_boolean('verbose', False, 'verbose or not in creating input data')
tf.app.flags.DEFINE_boolean('asgd', True, 'turn asgd on and off')
FLAGS = tf.app.flags.FLAGS

def extend_inp(data_path, predict_window, holiday_list):

    with tf.variable_scope('input', reuse=tf.AUTO_REUSE) as inp_scope:
        with tf.device("/cpu:0"):
            # inp = VarFeeder.read_vars("data/vars")
            inp = VarFeeder.read_vars(data_path)

            yesterday = pd.to_datetime(inp.data_end) + pd.Timedelta(predict_window, 'D')
            yesterday = yesterday.date().strftime('%Y-%m-%d')
            day = datetime.strptime(yesterday, '%Y-%m-%d')
            day_list = []
            for _ in range(0, inp.features_days + predict_window):
                day_list.append(datetime.strftime(day, '%Y-%m-%d'))
                day = day + timedelta(days=-1)
            day_list.sort()

            # computing lagged_ix
            date_range = pd.date_range(day_list[0], day_list[-1])
            base_index = pd.Series(np.arange(0, len(date_range)), index=date_range)

            def lag(offset):
                dates = date_range - offset
                return pd.Series(data=base_index[dates].fillna(-1).astype(np.int16).values, index=date_range)
            lagged_ix = np.stack([lag(pd.DateOffset(months=m)) for m in (1, 2)], axis=-1)

            with tf.Session() as sess:
                inp.restore(sess)
                # ts_log, page_ix_, pf_age_, pf_si_, pf_network_, pf_price_model_, \
                ts_log, page_ix_, pf_age_, pf_si_, pf_network_, pf_price_cat_, \
                pf_gender_, page_popularity_, quarter_autocorr_ = \
                    sess.run([inp.hits, inp.page_ix, inp.pf_age, inp.pf_si,
                              # inp.pf_network, inp.pf_price_model, inp.pf_gender,
                              inp.pf_network, inp.pf_price_cat, inp.pf_gender,
                              inp.page_popularity, inp.quarter_autocorr])
                print(f'start: {inp.data_start}\tend: {inp.data_end}\tlength: {inp.features_days}')

            df_ts = pd.DataFrame(np.append(ts_log, np.zeros((len(page_ix_), predict_window)), axis=1), index=list(page_ix_), columns=day_list)
            df_age = pd.DataFrame(pf_age_, index=list(page_ix_))
            df_si = pd.DataFrame(pf_si_, index=list(page_ix_))
            df_network = pd.DataFrame(pf_network_, index=list(page_ix_))
            # df_price_model = pd.DataFrame(pf_price_model_, index=list(page_ix_))
            df_price_cat = pd.DataFrame(pf_price_cat_, index=list(page_ix_))
            df_gender = pd.DataFrame(pf_gender_, index=list(page_ix_))

            def get_dow(day_list):
                dow_list = []
                for day in day_list:
                    dow = datetime.strptime(day, '%Y-%m-%d').weekday()
                    dow_list.append(dow)

                week_period = 7.0 / (2 * math.pi)
                sin_list = [math.sin(x / week_period) for x in dow_list]
                cos_list = [math.cos(x / week_period) for x in dow_list]
                return (sin_list, cos_list)

            dow_ = get_dow(day_list)

            # holiday_list = cfg['pipeline']['normalization']['holidays']

            holidays = [1 if _ in holiday_list else 0 for _ in day_list]
            a_list = []
            b_list = []
            for _ in holidays:
                a,b =math.sin(_), math.cos(_)
                a_list.append(a)
                b_list.append(b)
            holiday = (a_list, b_list)


            tensors = dict(
                hits=df_ts,
                lagged_ix=lagged_ix,
                page_ix=list(page_ix_),
                pf_age=df_age,
                pf_si=df_si,
                pf_network=df_network,
                # pf_price_model=df_price_model,
                pf_price_cat=df_price_cat,
                pf_gender=df_gender,
                page_popularity=page_popularity_,
                quarter_autocorr=quarter_autocorr_,
                dow=pd.DataFrame(dow_).T,
                holiday=pd.DataFrame(holiday).T
            )

            batch_size = len(page_ix_)
            data_len = tensors['hits'].shape[1]
            plain = dict(
                data_days=data_len - 0,
                features_days=data_len,
                data_start=day_list[0],
                data_end=day_list[-1],
                n_pages=batch_size
            )
            # dump_path = 'data/vars/predict_future'
            dump_path = os.path.join(data_path, 'predict_future')
            if not os.path.exists(dump_path):
                os.mkdir(dump_path)
            VarFeeder(dump_path, tensors, plain)
    tf.reset_default_graph()

def main(_):
  if len(sys.argv) < 3:
      print('Usage: saved_model.py [--model_version=y] --data_dir=xxx --ckpt_dir=xxx --saved_dir=xxx')
      sys.exit(-1)
  if FLAGS.training_iteration <= 0:
    print('Please specify a positive value for training iteration.')
    sys.exit(-1)
  if FLAGS.model_version <= 0:
    print('Please specify a positive value for version number.')
    sys.exit(-1)

  with open(FLAGS.config_file, 'r') as ymlfile:
      cfg = yaml.load(ymlfile)

  holiday_list = cfg['pipeline']['normalization']['holidays']
  if FLAGS.back_offset<FLAGS.predict_window:
      extend_inp(FLAGS.data_dir, FLAGS.predict_window, holiday_list)

  # create deploy model first
  back_offset_ = FLAGS.back_offset
  with tf.variable_scope('input') as inp_scope:
    with tf.device("/cpu:0"):
      if FLAGS.back_offset<FLAGS.predict_window:
        inp = VarFeeder.read_vars(os.path.join(FLAGS.data_dir, 'predict_future'))
        back_offset_ += FLAGS.predict_window
      else:
        inp = VarFeeder.read_vars(FLAGS.data_dir)
      pipe = InputPipe(inp, ucdoc_features(inp), inp.hits.shape[0], mode=ModelMode.PREDICT,
                       batch_size=FLAGS.batch_size, n_epoch=1, verbose=False,
                       train_completeness_threshold=0.01, predict_window=FLAGS.predict_window,
                       predict_completeness_threshold=0.0, train_window=FLAGS.train_window,
                       back_offset=back_offset_)

  asgd_decay = 0.99 if FLAGS.asgd else None

  if FLAGS.n_models == 1:
      model = Model(pipe, build_from_set(FLAGS.hparam_set), is_train=False, seed=1, asgd_decay=asgd_decay)
  else:
      models = []
      for i in range(FLAGS.n_models):
          prefix = f"m_{i}"
          with tf.variable_scope(prefix) as scope:
              models.append(Model(pipe, build_from_set(FLAGS.hparam_set), is_train=False, seed=1, asgd_decay=asgd_decay, graph_prefix=prefix))
      model = models[FLAGS.target_model]

  if FLAGS.asgd:
    var_list = model.ema.variables_to_restore()
    if FLAGS.n_models > 1:
      prefix = f"m_{target_model}"
      for var in list(var_list.keys()):
        if var.endswith('ExponentialMovingAverage') and not var.startswith(prefix):
          del var_list[var]
  else:
    var_list = None

  # load checkpoint model from training
  #ckpt_path = FLAGS.ckpt_dir
  print('loading checkpoint model...')
  ckpt_file = tf.train.latest_checkpoint(FLAGS.ckpt_dir)
  #graph = tf.Graph()
  graph = model.predictions.graph

  init = tf.global_variables_initializer()

  saver = tf.train.Saver(name='deploy_saver', var_list=var_list)
  with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:
    sess.run(init)
    pipe.load_vars(sess)
    pipe.init_iterator(sess)
    saver.restore(sess, ckpt_file)
    print('Done loading checkpoint model')
    export_path_base = FLAGS.saved_dir
    export_path = os.path.join(tf.compat.as_bytes(export_path_base), tf.compat.as_bytes(str(FLAGS.model_version)))
    print('Exporting trained model to', export_path)
    if os.path.isdir(export_path):
      shutil.rmtree(export_path)
    builder = tf.saved_model.builder.SavedModelBuilder(export_path)

    true_x = tf.saved_model.utils.build_tensor_info(model.inp.true_x) # pipe.true_x
    time_x = tf.saved_model.utils.build_tensor_info(model.inp.time_x) # pipe.time_x
    norm_x = tf.saved_model.utils.build_tensor_info(model.inp.norm_x) # pipe.norm_x
    lagged_x = tf.saved_model.utils.build_tensor_info(model.inp.lagged_x) # pipe.lagged_x
    true_y = tf.saved_model.utils.build_tensor_info(model.inp.true_y) # pipe.true_y
    time_y = tf.saved_model.utils.build_tensor_info(model.inp.time_y) # pipe.time_y
    norm_y = tf.saved_model.utils.build_tensor_info(model.inp.norm_y) # pipe.norm_y
    norm_mean = tf.saved_model.utils.build_tensor_info(model.inp.norm_mean) # pipe.norm_mean
    norm_std = tf.saved_model.utils.build_tensor_info(model.inp.norm_std) # pipe.norm_std
    pg_features = tf.saved_model.utils.build_tensor_info(model.inp.ucdoc_features) # pipe.ucdoc_features
    page_ix = tf.saved_model.utils.build_tensor_info(model.inp.page_ix) # pipe.page_ix

    #pred = tf.saved_model.utils.build_tensor_info(graph.get_operation_by_name('m_0/add').outputs[0])
    pred = tf.saved_model.utils.build_tensor_info(model.predictions)

    labeling_signature = (
      tf.saved_model.signature_def_utils.build_signature_def(
        inputs={
          "truex": true_x,
          "timex": time_x,
          "normx": norm_x,
          "laggedx": lagged_x,
          "truey": true_y,
          "timey": time_y,
          "normy": norm_y,
          "normmean": norm_mean,
          "normstd": norm_std,
          "page_features": pg_features,
          "pageix": page_ix,
        },
        outputs={
          "predictions": pred
        },
        method_name="tensorflow/serving/predict"))

    legacy_init_op = tf.group(tf.tables_initializer(), name='legacy_init_op')

    builder.add_meta_graph_and_variables(
      sess, [tf.saved_model.tag_constants.SERVING],
      signature_def_map={tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: labeling_signature},
      main_op=tf.tables_initializer(),
      strip_default_attrs=True)

    builder.save()
    print("Build Done")


if __name__ == '__main__':
  tf.app.run()
