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

import os
import time
import pickle
import random
import numpy as np
import tensorflow as tf
import sys
from lookalike_model.trainer.input import DataInput, DataInputTest
from model import Model
from sklearn.metrics import roc_auc_score


# os.environ['CUDA_VISIBLE_DEVICES'] = "0,1,2,3"
random.seed(1234)
np.random.seed(1234)
tf.set_random_seed(1234)

train_batch_size = 20480
test_batch_size = 2048
predict_batch_size = 512
predict_users_num = 1000
predict_ads_num = 15
time_interval_num = 10

NUM_OF_EPOCHS = 250


def calc_auc(raw_arr):
    """Summary

    Args:
        raw_arr (TYPE): Description

    Returns:
        TYPE: Description
    """
    # sort by pred value, from small to big
    arr = sorted(raw_arr, key=lambda d: d[2])

    auc = 0.0
    fp1, tp1, fp2, tp2 = 0.0, 0.0, 0.0, 0.0
    for record in arr:
        fp2 += record[0]  # noclick
        tp2 += record[1]  # click
        auc += (fp2 - fp1) * (tp2 + tp1)
        fp1, tp1 = fp2, tp2

    # if all nonclick or click, disgard
    threshold = len(arr) - 1e-3
    if tp2 > threshold or fp2 > threshold:
        return -0.5

    if tp2 * fp2 > 0.0:  # normal auc
        return (1.0 - auc / (2.0 * tp2 * fp2))
    else:
        return None


def _eval_logdata(sess, model, next_el, epoch_size, type):
    score_arr = []
    y = []
    for _ in range(epoch_size):
        uij = sess.run(next_el)
        score_ = model.eval_logdata(sess, uij)
        score_arr.append(np.squeeze(score_[0]))
        y.append(np.asarray(uij[2]))
    score_arr = np.hstack(score_arr)
    y = np.hstack(np.asarray(y))
    Auc = roc_auc_score(y, score_arr)

    global best_auc_train
    global best_auc_test

    if type == 'train' and best_auc_train < Auc:
        best_auc_train = Auc
    if type == 'test' and best_auc_test < Auc:
        best_auc_test = Auc
        model.save(sess, 'save_path/ckpt')
    return Auc


def __data_parser(serialized_example):

    features = tf.parse_single_example(serialized_example,
                                       features={
                                           'keyword_list_padded': tf.FixedLenSequenceFeature([], tf.int64, allow_missing=True),
                                           'aid': tf.FixedLenFeature([], tf.int64),
                                           'keyword': tf.FixedLenFeature([], tf.int64),
                                           'label': tf.FixedLenFeature([], tf.int64),
                                           'sl': tf.FixedLenFeature([], tf.int64)
                                       })

    keyword_list_padded = tf.cast(features['keyword_list_padded'], tf.int64)
    ucdoc = tf.cast(features['aid'], tf.int64)
    keyword = tf.cast(features['keyword'], tf.int64)
    is_click = tf.cast(features['label'], tf.int64)
    sl = tf.cast(features['sl'], tf.int64)

    return ucdoc, keyword, is_click, keyword_list_padded, sl


# Reading tfrecord as a dataset
def read_dataset(tfrecord_location):
    names = []
    for file in os.listdir(tfrecord_location):
        if file.startswith("part"):
            names.append(file)
    file_paths = [os.path.join(tfrecord_location, name) for name in names]
    dataset = tf.data.TFRecordDataset(file_paths)
    dataset = dataset.map(__data_parser)
    dataset = dataset.batch(train_batch_size).repeat()
    iterator = dataset.make_one_shot_iterator()
    next_el = iterator.get_next()
    return dataset, next_el


if __name__ == '__main__':

    # read the following from arg or other sources
    # inputs
    stats = "lookalike_11192021_tfrecord_statistics.pkl"
    tfrecord_location = "lookalike_tfrecords_train_lookalike_faezeh"
    tfrecord_location_test = "lookalike_tfrecords_test_lookalike_faezeh"

    train_data, next_el = read_dataset(tfrecord_location)
    test_data, next_el_test = read_dataset(tfrecord_location_test)

    with open(stats, 'rb') as f:
        stat = pickle.load(f)

    user_count, item_count, cate_count, total_data, total_data_test = stat['user_count'], stat[
        'item_count'], stat['item_count'], stat['train_dataset_count'], stat['test_dataset_count']
    cate_list = [i for i in range(cate_count)]

    test_set_with_label = []

    best_auc = 0.0
    best_auc_train = 0.0
    best_auc_test = 0.0

    if min(cate_list) > 0:
        item_count += 1

    gpu_options = tf.GPUOptions(allow_growth=True)
    with tf.Session(config=tf.ConfigProto(gpu_options=gpu_options)) as sess:

        model = Model(user_count, item_count, cate_count, cate_list, predict_batch_size, predict_ads_num)
        sess.run(tf.global_variables_initializer())
        sess.run(tf.local_variables_initializer())

        lr = 1
        start_time = time.time()
        counter = 0
        for _ in range(NUM_OF_EPOCHS):
            loss_sum = 0.0
            cnt = 0
            epoch_total_loss = 0.0
            epoch_size = round(total_data / train_batch_size)
            if epoch_size * train_batch_size < total_data:
                epoch_size += 1

            for _ in range(epoch_size):
                uij = sess.run(next_el)
                uij = (list(uij[0]), list(uij[1]), list(uij[2]), np.array(uij[3]), list(uij[4]))
                loss = model.train(sess, uij, lr)
                loss_sum += loss
                epoch_total_loss += loss
                cnt += 1

                if model.global_step.eval() % 336000 == 0:
                    lr = 1.0

            epoch_mean_loss = epoch_total_loss / cnt

            if counter % 3 == 0:
                auc_train = _eval_logdata(sess, model, next_el, epoch_size, 'train')
                epoch_size_test = round(total_data_test/test_batch_size) + 1
                auc_test = _eval_logdata(sess, model, next_el_test, epoch_size_test, 'test')
                print('Epoch %d DONE\tGlobal step: %d\tCost time: %.2f\ttrain epoch average loss: %.6f\ttrain auc: %.4f\ttest auc: %.4f' %
                      (model.global_epoch_step.eval(), model.global_step.eval(), time.time()-start_time, epoch_mean_loss, auc_train, auc_test))
            counter += 1
            sys.stdout.flush()
            model.global_epoch_step_op.eval()

        print('best train_gauc %.4f\ttest_gauc: %.4f' % (best_auc_train, best_auc_test))
        sys.stdout.flush()
