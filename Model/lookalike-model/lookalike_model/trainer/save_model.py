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

# ! /usr/bin/env python
#  generate tensorflow serving servable model from trained checkpoint model.
#
#  The program uses information from input data and trained checkpoint model
#  to generate TensorFlow SavedModel that can be loaded by standard
#  tensorflow_model_server.
#
#  sample command:
#      python save_model.py --predict_ads_num=30 --data_dir=./ --ckpt_dir=./save_path --saved_dir=./save_path --model_version=1
#
#      the resultant saved_model will locate at ./save_path/1

from __future__ import print_function

import os
import sys

# This is a placeholder for a Google-internal import.

import tensorflow as tf
import shutil
from model import Model
from collections import Iterable
import pickle

tf.app.flags.DEFINE_integer('model_version', 1, 'version number of the model.')
tf.app.flags.DEFINE_integer('predict_batch_size', 512, 'batch size of prediction.')
tf.app.flags.DEFINE_integer('predict_ads_num', 15, 'number of ads in prediction.')
tf.app.flags.DEFINE_string('saved_dir', './save_path', 'directory to save generated tfserving model.')
tf.app.flags.DEFINE_string('ckpt_dir', default='./save_path', help='checkpint directory')
tf.app.flags.DEFINE_string('stat_dir', default='.', help='data file directory which contains cate_list')
tf.app.flags.DEFINE_string('cate_list_fl', 'data/vars', 'input data directory')
FLAGS = tf.app.flags.FLAGS


def main(_):
    if len(sys.argv) < 3:
        print('Usage: saved_model.py [--model_version=y] --stat_dir=xxx --ckpt_dir=xxx --saved_dir=xxx')
        sys.exit(-1)
    if FLAGS.model_version <= 0:
        print('Please specify a positive value for version number.')
        sys.exit(-1)

    stat = os.path.join(FLAGS.stat_dir, 'lookalike_01202022_tfrecord_statistics.pkl')
    with open(stat, 'rb') as f:
        stat = pickle.load(f)
    user_count, item_count, cate_count, total_data, total_data_test = stat['user_count'], stat['item_count'], stat[
        'item_count'], stat['train_dataset_count'], stat['test_dataset_count']
    cate_list = [i for i in range(cate_count)]

    model = Model(user_count, item_count, cate_count, cate_list, FLAGS.predict_batch_size, FLAGS.predict_ads_num)

    # load checkpoint model from training
    print('loading checkpoint model...')
    ckpt_file = tf.train.latest_checkpoint(FLAGS.ckpt_dir)

    saver = tf.train.Saver(name='deploy_saver', var_list=None)
    with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:
        # pipe.load_vars(sess)
        # pipe.init_iterator(sess)
        # model = Model(user_count, item_count, cate_count, cate_list, predict_batch_size, predict_ads_num)
        sess.run(tf.global_variables_initializer())
        sess.run(tf.local_variables_initializer())

        saver.restore(sess, ckpt_file)
        print('Done loading checkpoint model')
        export_path_base = FLAGS.saved_dir
        export_path = os.path.join(tf.compat.as_bytes(export_path_base), tf.compat.as_bytes(str(FLAGS.model_version)))
        print('Exporting trained model to', export_path)
        if os.path.isdir(export_path):
            shutil.rmtree(export_path)
        builder = tf.saved_model.builder.SavedModelBuilder(export_path)

        u = tf.saved_model.utils.build_tensor_info(model.u)
        i = tf.saved_model.utils.build_tensor_info(model.i)
        j = tf.saved_model.utils.build_tensor_info(model.j)
        y = tf.saved_model.utils.build_tensor_info(model.y)
        hist_i = tf.saved_model.utils.build_tensor_info(model.hist_i)
        sl = tf.saved_model.utils.build_tensor_info(model.sl)
        lr = tf.saved_model.utils.build_tensor_info(model.lr)

        # pred = tf.saved_model.utils.build_tensor_info(graph.get_operation_by_name('m_0/add').outputs[0])
        pred = tf.saved_model.utils.build_tensor_info(model.score_i)

        labeling_signature = (
            tf.saved_model.signature_def_utils.build_signature_def(
                inputs={
                    "u": u,
                    "i": i,
                    "j": j,
                    # "y": y,
                    "hist_i": hist_i,
                    "sl": sl,
                    # "lr": lr,
                },
                outputs={
                    "pred": pred
                },
                method_name="tensorflow/serving/predict"))

        legacy_init_op = tf.group(tf.tables_initializer(), name='legacy_init_op')

        builder.add_meta_graph_and_variables(
            sess, [tf.saved_model.tag_constants.SERVING],
            signature_def_map={
                tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: labeling_signature},
            main_op=tf.tables_initializer(),
            strip_default_attrs=True)

        builder.save()
        print("Build Done")


if __name__ == '__main__':
    tf.app.run()
