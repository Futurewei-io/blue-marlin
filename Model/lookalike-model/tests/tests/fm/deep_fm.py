import pickle
import argparse
import tensorflow as tf
from tensorflow.keras import Model
from tensorflow.keras.regularizers import l2
from tensorflow.keras.layers import Embedding, Dropout, Dense, Input, Layer
import numpy as np


class FM(Layer):
    def __init__(self, hyperparams):
        super(FM, self).__init__()
        fm = hyperparams["FM"]
        self.k = fm["k"]
        self.w_reg = fm["w_reg"]
        self.v_reg = fm["v_reg"]

    def build(self, input_shape):
        self.w0 = self.add_weight(name='w0', shape=(1,),
                                  initializer=tf.zeros_initializer(),
                                  trainable=True)
        self.w = self.add_weight(name='w', shape=(input_shape[-1], 1),
                                 initializer='random_uniform',
                                 regularizer=l2(self.w_reg),
                                 trainable=True)
        self.V = self.add_weight(name='V', shape=(self.k, input_shape[-1]),
                                 initializer='random_uniform',
                                 regularizer=l2(self.v_reg),
                                 trainable=True)

    def call(self, inputs, **kwargs):
        # first order
        first_order = self.w0 + tf.matmul(inputs, self.w)
        # second order
        second_order = 0.5 * tf.reduce_sum(
            tf.pow(tf.matmul(inputs, tf.transpose(self.V)), 2) -
            tf.matmul(tf.pow(inputs, 2), tf.pow(tf.transpose(self.V), 2)), axis=1, keepdims=True)
        return first_order + second_order


class DNN(Layer):
    def __init__(self, hyperparams):
        super(DNN, self).__init__()
        dnn_params = hyperparams["DNN_PARAMS"]
        self.dnn_network = [Dense(units=unit, activation=dnn_params["activation"]) for unit in
                            dnn_params["hidden_units"]]
        self.dropout = Dropout(dnn_params["dnn_dropout"])

    def call(self, inputs, **kwargs):
        x = inputs
        for dnn in self.dnn_network:
            x = dnn(x)
        x = self.dropout(x)
        return x


class DeepFM(Model):
    def __init__(self, hyperparams):
        super(DeepFM, self).__init__()
        features = hyperparams["DEEP_FM_FEATURES"]
        self.embed_layers = []
        for name, conf in features.items():
            self.embed_layers.append(tf.keras.layers.Embedding(input_dim=conf["unique_count"] + 1,
                                                               output_dim=conf["emmbedding_dim"],
                                                               name=name,
                                                               embeddings_regularizer=l2(conf["embed_reg"])))
        self.fm = FM(hyperparams)
        self.dnn = DNN(hyperparams)
        self.dense = Dense(1, activation=None)

    def call(self, inputs, **kwargs):
        featurs_len = len(self.embed_layers)
        dense_feature_list = [self.embed_layers[i](tf.gather(inputs, indices=[i], axis=1)) for i in range(featurs_len)]
        stack = tf.concat(dense_feature_list, axis=-1)
        wide_outputs = self.fm(stack)
        deep_outputs = self.dnn(stack)
        deep_outputs = self.dense(deep_outputs)
        outputs = tf.nn.sigmoid(tf.add(wide_outputs, deep_outputs))
        outputs = tf.reshape(outputs, shape=(1, -1))
        return outputs

    def summary(self):
        dense_inputs = Input(shape=(len(self.embed_layers),), dtype=tf.int32)
        Model(inputs=[dense_inputs], outputs=self.call([dense_inputs])).summary()


class TF_Record_reader():
    def __init__(self, hyperparams, tfrecord_location):
        super(TF_Record_reader, self).__init__()
        self.tfrecord_location = tfrecord_location
        self.reader_conf = hyperparams["TF_RECORDS_READER"]

    def __data_parser(self, serialized_example):
        features = tf.io.parse_single_example(serialized_example,
                                              features={'features': tf.io.FixedLenSequenceFeature([], tf.int64,
                                                                                                  allow_missing=True),
                                                        'is_click': tf.io.FixedLenFeature([], tf.int64),
                                                        })
        features_list = tf.cast(features['features'], tf.int64)
        is_click = tf.cast(features['is_click'], tf.int64)
        return features_list, is_click

    def get_dataset(self):
        files = tf.data.Dataset.list_files(self.tfrecord_location)
        raw_dataset = tf.data.TFRecordDataset(files)
        dataset = raw_dataset.map(self.__data_parser)
        dataset = dataset.shuffle(self.reader_conf["shuffle"]).batch(self.reader_conf["batch"]).prefetch(
            self.reader_conf["prefetch"])
        return dataset

