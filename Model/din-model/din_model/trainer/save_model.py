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

"""generate tensorflow serving servable model from trained checkpoint model.

The program uses information from input data and trained checkpoint model
to generate TensorFlow SavedModel that can be loaded by standard
tensorflow_model_server.

sample command:
    python save_model.py --ckpt_dir=save_path--saved_dir=/tmp/tfserving/serving/servable/ucdoc --model_version=1
"""

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
tf.app.flags.DEFINE_integer('predict_batch_size', 32, 'batch size of prediction.')
tf.app.flags.DEFINE_integer('predict_ads_num', 100, 'number of ads in prediction.')
tf.app.flags.DEFINE_integer('time_interval_num', 30, 'number of time interval in history.')
tf.app.flags.DEFINE_string('saved_dir', './save_path', 'directory to save generated tfserving model.')
tf.app.flags.DEFINE_string('ckpt_dir', default='./save_path', help='checkpint directory')
tf.app.flags.DEFINE_string('data_dir', default='./', help='data file directory which contains cate_list')
tf.app.flags.DEFINE_string('cate_list_fl', 'data/vars', 'input data directory')
FLAGS = tf.app.flags.FLAGS


def main(_):
    if len(sys.argv) < 3:
        print('Usage: saved_model.py [--model_version=y] --data_dir=xxx --ckpt_dir=xxx --saved_dir=xxx')
        sys.exit(-1)
    if FLAGS.model_version <= 0:
        print('Please specify a positive value for version number.')
        sys.exit(-1)

    fn_data = os.path.join(FLAGS.data_dir, 'data/ad_dataset_gdin.' + str(FLAGS.time_interval_num) + '.pkl')
    with open(fn_data, 'rb') as f:
        _ = pickle.load(f)
        _ = pickle.load(f)
        cate_list = pickle.load(f)
        user_count, item_count, cate_count = pickle.load(f)

    model = Model(user_count, item_count, cate_count, cate_list, FLAGS.predict_batch_size, FLAGS.predict_ads_num)

    # load checkpoint model from training
    print('loading checkpoint model...')
    ckpt_file = tf.train.latest_checkpoint(FLAGS.ckpt_dir)

    saver = tf.train.Saver(name='deploy_saver', var_list=None)
    with tf.Session(config=tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))) as sess:
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
        hist_i = tf.saved_model.utils.build_tensor_info(model.hist_i)
        sl = tf.saved_model.utils.build_tensor_info(model.sl)

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

        builder.add_meta_graph_and_variables(
            sess, [tf.saved_model.tag_constants.SERVING],
            signature_def_map={tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: labeling_signature},
            main_op=tf.tables_initializer(),
            strip_default_attrs=True)

        builder.save()
        print("Build Done")


if __name__ == '__main__':
    tf.app.run()
