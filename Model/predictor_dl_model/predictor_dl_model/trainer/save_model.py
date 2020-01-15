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

tf.app.flags.DEFINE_integer('training_iteration', 1000, 'number of training iterations.')
tf.app.flags.DEFINE_string('hparam_set', default='s32', help="Hyperparameters set to use (see hparams.py for available sets)")
tf.app.flags.DEFINE_integer('batch_size', 1024, 'number of sample in each batch')
tf.app.flags.DEFINE_integer('model_version', 1, 'version number of the model.')
tf.app.flags.DEFINE_integer('target_model', 1, 'the index of model to use.')
tf.app.flags.DEFINE_integer('n_models', 1, 'number of the model.')
tf.app.flags.DEFINE_integer('train_window', 20, 'number of time spots in training')
tf.app.flags.DEFINE_integer('predict_window', 3, 'Number of days to predict')
tf.app.flags.DEFINE_string('saved_dir', '/tmp/faezeh', 'directory to save generated tfserving model.')
tf.app.flags.DEFINE_string('ckpt_dir', default='data/cpt/s32', help='checkpint directory')
tf.app.flags.DEFINE_string('data_dir', 'data/vars', 'input data directory')
tf.app.flags.DEFINE_boolean('verbose', False, 'verbose or not in creating input data')
tf.app.flags.DEFINE_boolean('asgd', False, 'turn asgd on and off')
FLAGS = tf.app.flags.FLAGS


def main(_):
  if len(sys.argv) < 3:
      print('Usage: ucdoc_saved_model.py [--model_version=y] --data_dir=xxx --ckpt_dir=xxx --saved_dir=xxx')
      sys.exit(-1)
  if FLAGS.training_iteration <= 0:
    print('Please specify a positive value for training iteration.')
    sys.exit(-1)
  if FLAGS.model_version <= 0:
    print('Please specify a positive value for version number.')
    sys.exit(-1)

  # create deploy model first
  with tf.variable_scope('input') as inp_scope:
    with tf.device("/cpu:0"):
      #inp = VarFeeder.read_vars("data/vars")
      inp = VarFeeder.read_vars(FLAGS.data_dir)
      pipe = InputPipe(inp, ucdoc_features(inp), inp.hits.shape[0], mode=ModelMode.PREDICT,
                       batch_size=FLAGS.batch_size, n_epoch=1, verbose=False,
                       train_completeness_threshold=0.01, predict_window=FLAGS.predict_window,
                       predict_completeness_threshold=0.0, train_window=FLAGS.train_window,
                       back_offset=FLAGS.predict_window+1)

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

  # load checkpoint model from training
  #ckpt_path = FLAGS.ckpt_dir
  print('loading checkpoint model...')
  ckpt_file = tf.train.latest_checkpoint(FLAGS.ckpt_dir)
  #graph = tf.Graph()
  graph = model.predictions.graph

  saver = tf.train.Saver(name='deploy_saver', var_list=None)
  with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:
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

    true_x = tf.saved_model.utils.build_tensor_info(model.inp.true_x)
    time_x = tf.saved_model.utils.build_tensor_info(model.inp.time_x)
    norm_x = tf.saved_model.utils.build_tensor_info(model.inp.norm_x)
    lagged_x = tf.saved_model.utils.build_tensor_info(model.inp.lagged_x)
    true_y = tf.saved_model.utils.build_tensor_info(model.inp.true_y)
    time_y = tf.saved_model.utils.build_tensor_info(model.inp.time_y)
    norm_y = tf.saved_model.utils.build_tensor_info(model.inp.norm_y)
    norm_mean = tf.saved_model.utils.build_tensor_info(model.inp.norm_mean)
    norm_std = tf.saved_model.utils.build_tensor_info(model.inp.norm_std)
    pg_features = tf.saved_model.utils.build_tensor_info(model.inp.ucdoc_features)
    page_ix = tf.saved_model.utils.build_tensor_info(model.inp.page_ix)

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
          "pred": pred
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
