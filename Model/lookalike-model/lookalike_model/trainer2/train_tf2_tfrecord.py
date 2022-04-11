import tensorflow as tf
from time import time
from tensorflow.keras.losses import binary_crossentropy
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.metrics import AUC
import time

from model_tf2 import DIN
from utils_tf2 import *

import os
import pickle
import numpy as np
import glob

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['CUDA_VISIBLE_DEVICES'] = '0,1,2,3,4,5,6,7'


def parse_example(serialized_example, max_len):
    expected_features = {
        'keyword_list_padded': tf.io.FixedLenSequenceFeature([max_len], dtype=tf.int64, allow_missing=True),
        'aid': tf.io.FixedLenSequenceFeature([1], dtype=tf.int64, allow_missing=True),
        'keyword': tf.io.FixedLenSequenceFeature([1], tf.int64, allow_missing=True),
        'label': tf.io.FixedLenSequenceFeature([1], tf.int64, allow_missing=True),
        'sl': tf.io.FixedLenSequenceFeature([1], tf.int64, allow_missing=True)
    }
    example = tf.io.parse_single_example(serialized_example, features=expected_features)
    keyword_list_padded = tf.squeeze(tf.repeat(tf.expand_dims(tf.cast(example['keyword_list_padded'], dtype=tf.int64), -1), repeats=[2], axis=2))
    uid = tf.squeeze(tf.cast(example['aid'], dtype=tf.int64))
    keyword = tf.squeeze(tf.repeat(tf.expand_dims(tf.cast(example['keyword'], dtype=tf.int64), -1), repeats=[2], axis=1))
    is_click = tf.squeeze(tf.cast(example['label'], dtype=tf.int64))
    sl = tf.squeeze(tf.cast(example['sl'], dtype=tf.int64))
    return keyword_list_padded, uid, keyword, is_click, sl

def tfrecord_reader_dataset(tfrecord_location, batch_size, max_len):
    names = []
    for file in os.listdir(tfrecord_location):
        if file.startswith("part-r-"):
            names.append(file)
    file_paths = sorted([os.path.join(tfrecord_location, name) for name in names])
    dataset = tf.data.TFRecordDataset(file_paths)
    # dataset = dataset.map(parse_example)
    dataset = dataset.map(lambda x: parse_example(x, max_len))
    batched_dataset = dataset.batch(batch_size).repeat(1)
    return batched_dataset


if __name__ == '__main__':
    tfrecord_location_train = 'lookalike_02022022_train_tfrecord'
    tfrecord_location_test = 'lookalike_02022022_test_tfrecord'

    item_count = 16

    # ========================= Hyper Parameters =======================
    embed_dim = 8
    att_hidden_units = [80, 40]
    ffn_hidden_units = [256, 128, 64]
    dnn_dropout = 0.5
    att_activation = 'sigmoid'
    ffn_activation = 'prelu'

    learning_rate = 0.001
    batch_size_train = 20480*2
    batch_size_test = 10240
    epochs = 50
    maxlen_train, maxlen_test = 99, 79

    batched_dataset_train = tfrecord_reader_dataset(tfrecord_location_train, batch_size_train, maxlen_train)
    batched_dataset_test = tfrecord_reader_dataset(tfrecord_location_test, batch_size_test, maxlen_test)

    hist_test, curr_test, label_test = [], [], []
    cnt_ = 0
    for next_element_test in batched_dataset_test:
        print(f'reading {cnt_}th batch in testing dataset')
        hist_, _, curr_, label_, _ = next_element_test
        hist_test.append(hist_.numpy())
        curr_test.append(curr_.numpy())
        label_test.append(label_.numpy())
        cnt_ += 1
    print('finish reading testing dataset')
    hist_test = pad_sequences(np.concatenate(hist_test, axis=0), maxlen=maxlen_train)
    curr_test = np.concatenate(curr_test, axis=0)
    label_test = np.concatenate(label_test)
    test_X = [np.array([0] * hist_test.shape[0]), np.array([0] * hist_test.shape[0]),
              hist_test.astype(np.int32), curr_test]
    test_y = label_test

    feature_columns = [[], [sparseFeature('item_id', item_count, embed_dim),]]
    behavior_list = ['item_id']

    s = time.time()
    # ============================Build Model==========================
    model = DIN(feature_columns, behavior_list, att_hidden_units, ffn_hidden_units, att_activation,
        ffn_activation, maxlen_train, dnn_dropout)
    model.summary()
    # =========================Compile============================
    model.compile(loss=binary_crossentropy, optimizer=Adam(learning_rate=learning_rate),
                  metrics=[AUC()])

    # ===========================Fit==============================
    batch_cnt = 0
    for next_element in batched_dataset_train:
        hist_, _, curr_, labl_, _ = next_element
        batch_size_here = labl_.numpy().shape[0]
        print(f'***********************compute batch {batch_cnt} batch size {batch_size_here}***********************')
        model.fit(
            [np.zeros(batch_size_here), np.zeros(batch_size_here),
             hist_.numpy().astype(np.int32), curr_.numpy()],
            labl_.numpy(),
            # epochs=1,
            validation_data=(test_X, test_y),
            batch_size=batch_size_here,
        )
        batch_cnt += 1

    # ===========================Test==============================
    print('test AUC: %f' % model.evaluate(test_X, test_y, batch_size=batch_size_test)[1])
    print(f'elapse: {time.time()-s} secs')