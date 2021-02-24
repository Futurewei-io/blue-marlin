# Copyright 2020, Futurewei Technologies
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

import tensorflow as tf

def dice(_x, axis=-1, epsilon=0.000000001, name=''):
  with tf.variable_scope(name_or_scope='', reuse=tf.AUTO_REUSE):
    alphas = tf.get_variable('alpha'+name, _x.get_shape()[-1],                                  
                         initializer=tf.constant_initializer(0.0),                         
                         dtype=tf.float32)
    beta = tf.get_variable('beta'+name, _x.get_shape()[-1],                                  
                         initializer=tf.constant_initializer(0.0),                         
                         dtype=tf.float32)
  input_shape = list(_x.get_shape())

  reduction_axes = list(range(len(input_shape)))
  del reduction_axes[axis]
  broadcast_shape = [1] * len(input_shape)
  broadcast_shape[axis] = input_shape[axis]
                                                                                                                                                                            
  # case: train mode (uses stats of the current batch)
  mean = tf.reduce_mean(_x, axis=reduction_axes)
  brodcast_mean = tf.reshape(mean, broadcast_shape)
  std = tf.reduce_mean(tf.square(_x - brodcast_mean) + epsilon, axis=reduction_axes)
  std = tf.sqrt(std)
  x_normed = tf.layers.batch_normalization(_x, center=False, scale=False, name=name, reuse=tf.AUTO_REUSE)
  x_p = tf.sigmoid(beta * x_normed)
 
  
  return alphas * (1.0 - x_p) * _x + x_p * _x

def parametric_relu(_x):
  with tf.variable_scope(name_or_scope='', reuse=tf.AUTO_REUSE):
    alphas = tf.get_variable('alpha', _x.get_shape()[-1],
                         initializer=tf.constant_initializer(0.0),
                         dtype=tf.float32)
  pos = tf.nn.relu(_x)
  neg = alphas * (_x - abs(_x)) * 0.5

  return pos + neg
