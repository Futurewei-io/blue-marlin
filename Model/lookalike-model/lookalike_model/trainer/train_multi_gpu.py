import os
os.environ['CUDA_VISIBLE_DEVICES'] = "0,1,2,3"

import time
import pickle
import random
import numpy as np
import tensorflow as tf
import sys
from input import DataInput, DataInputTest
from model_multi_gpu import Model, DIN
from sklearn.metrics import roc_auc_score

random.seed(1234)
np.random.seed(1234)
tf.set_random_seed(1234)

train_batch_size = 2048 #1024
test_batch_size = 2048 #1024
predict_batch_size = 32
predict_users_num = 1000
# predict_ads_num = 6
predict_ads_num = 30
time_interval_num = 10
gpu_num = 3

with open('ad_dataset_lookalike_jimmy_' + str(time_interval_num) + '.pkl', 'rb') as f:
  train_set = pickle.load(f)
  test_set = pickle.load(f)
  cate_list = pickle.load(f)
  user_count, item_count, cate_count = pickle.load(f)

with open('label_lookalike_jimmy_' + str(time_interval_num) + '.pkl', 'rb') as f:
    test_lb = pickle.load(f)
test_set_with_label = []
for i in range(len(test_lb)):
    test_set_with_label.append(tuple([test_set[i][0], test_set[i][1], test_set[i][2][0], test_lb[i]]))

best_auc = 0.0
best_auc_train = 0.0
best_auc_test = 0.0
def calc_auc(raw_arr):
    """Summary

    Args:
        raw_arr (TYPE): Description

    Returns:
        TYPE: Description
    """
    # sort by pred value, from small to big
    arr = sorted(raw_arr, key=lambda d:d[2])

    auc = 0.0
    fp1, tp1, fp2, tp2 = 0.0, 0.0, 0.0, 0.0
    for record in arr:
        fp2 += record[0] # noclick
        tp2 += record[1] # click
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


def _auc_arr(score):
  score_p = score[:,0]
  score_n = score[:,1]
  #print "============== p ============="
  #print score_p
  #print "============== n ============="
  #print score_n
  score_arr = []
  for s in score_p.tolist():
    score_arr.append([0, 1, s])
  for s in score_n.tolist():
    score_arr.append([1, 0, s])
  return score_arr


def _eval(sess, model):
  auc_sum = 0.0
  score_arr = []
  for _, uij in DataInputTest(test_set, test_batch_size):
    auc_, score_ = model.eval(sess, uij)
    score_arr += _auc_arr(score_)
    auc_sum += auc_ * len(uij[0])
  test_gauc = auc_sum / len(test_set)
  Auc = calc_auc(score_arr)
  global best_auc
  if best_auc < test_gauc:
    best_auc = test_gauc
    model.save(sess, 'save_path/ckpt')
  return test_gauc, Auc


def _eval_logdata(sess, model, dataset, batch_size, type):
    score_arr = []
    y = []
    for _, uij in DataInput(dataset, batch_size):
        score_ = model.eval_logdata(sess, uij)
        score_arr.append(np.squeeze(score_[0]))
        y.append(np.asarray(uij[2]))
    # score_arr = np.vstack(score_arr)
    score_arr = np.hstack(score_arr)
    y = np.hstack(np.asarray(y))
    Auc = roc_auc_score(y, score_arr)

    global best_auc_train
    global best_auc_test

    if type=='train' and best_auc_train<Auc:
        best_auc_train = Auc
    if type=='test' and best_auc_test<Auc:
        best_auc_test = Auc
        model.save(sess, 'save_path/ckpt')
    return Auc

def _eval_logdata_improve(sess, dataset, batch_size, type):
    score_arr = []
    y = []
    for _, uij in DataInput(dataset, batch_size):
        score_ = sess.run([batch_logits_test], feed_dict={
            i_batch: uij[1],
            j_batch: uij[2],
            y_batch: uij[2],
            hist_i_batch: uij[3],
            sl_batch: uij[4]
        })

        score_arr.append(np.squeeze(score_[0]))
        y.append(np.asarray(uij[2]))
    score_arr = np.hstack(score_arr)
    y = np.hstack(np.asarray(y))
    Auc = roc_auc_score(y[:len(score_arr)], score_arr)

    global best_auc_train
    global best_auc_test

    if type=='train' and best_auc_train<Auc:
        best_auc_train = Auc
    if type=='test' and best_auc_test<Auc:
        best_auc_test = Auc
        saver = tf.train.Saver()
        saver.save(sess, save_path='save_path/ckpt')

    return Auc

def _test(sess, model):
  auc_sum = 0.0
  score_arr = []
  predicted_users_num = 0
  print("test sub items")
  for _, uij in DataInputTest(test_set, predict_batch_size):
    if predicted_users_num >= predict_users_num:
        break
    score_ = model.test(sess, uij)
    score_arr.append(score_)
    predicted_users_num += predict_batch_size
  return score_[0]


PS_OPS = ['Variable', 'VariableV2', 'AutoReloadVariable']
def assign_to_device(device, ps_device='/cpu:0'):
    def _assign(op):
        node_def = op if isinstance(op, tf.NodeDef) else op.node_def
        if node_def.op in PS_OPS:
            return "/" + ps_device
        else:
            return device

    return _assign


def average_gradients(tower_grads):
    average_grads, cnt = [], 0
    for grad_and_vars in zip(*tower_grads):
        grads = []
        for g, _ in grad_and_vars:
            if g is not None:
                expanded_g = tf.expand_dims(g, 0)
            else:
                expanded_g = None

            grads.append(expanded_g)

        if all(x is None for x in grads):
            grad = None
        else:
            grad = tf.concat(grads, 0)
            grad = tf.reduce_mean(grad, 0)

        v = grad_and_vars[0][1]
        grad_and_var = (grad, v)
        average_grads.append(grad_and_var)
        cnt += 1

        rlt_grads = [v[0] for v in average_grads]
        rlt_vars = [v[1] for v in average_grads]
    return average_grads
    # return rlt_grads, rlt_vars

if min(cate_list)>0:
    item_count += 1

lr = 1.0  # 0.1
reuse_vars = False

tf.reset_default_graph()

with tf.device('/cpu:0'):
    tower_grads, tower_logits_test = [], []
    reuse_vars = False

    # tf Graph input
    i_batch = tf.placeholder(tf.int32, [None,]) # [B]
    j_batch = tf.placeholder(tf.int32, [None,]) # [B]
    y_batch = tf.placeholder(tf.int32, [None,]) # [B]
    hist_i_batch = tf.placeholder(tf.int32, [None, None]) # [B, T]
    sl_batch = tf.placeholder(tf.int32, [None,]) # [B]

    for i in range(gpu_num):
        with tf.device(assign_to_device('/gpu:{}'.format(i), ps_device='/cpu:0')):
            delta = tf.shape(i_batch)[0] // gpu_num
            start, end = i*delta, (i+1)*delta
            i = i_batch[start:end]
            j = j_batch[start:end]
            y = y_batch[start:end]
            hist_i = hist_i_batch[start:end,:]
            sl = sl_batch[start:end]

            logits_train = DIN(i, j, y, hist_i, sl,
                               item_count, cate_count, cate_list, reuse=reuse_vars, is_training=True)

            logits_test = DIN(i, j, y, hist_i, sl,
                              item_count, cate_count, cate_list, reuse=True, is_training=False)

            loss_op = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(
                logits=logits_train, labels=tf.cast(y, tf.float32)))

            trainable_params = tf.trainable_variables()

            gradients = tf.gradients(loss_op, trainable_params)
            clip_gradients, _ = tf.clip_by_global_norm(gradients, 5)

            grads = [(v, trainable_params[ind]) for ind, v in enumerate(clip_gradients)]

            reuse_vars = True
            tower_grads.append(grads)
            tower_logits_test.append(logits_test)

    batch_logits_test = tf.concat(tower_logits_test, 0)
    average_grads = average_gradients(tower_grads)
    optimizer = tf.train.GradientDescentOptimizer(learning_rate=lr)

    train_op = optimizer.apply_gradients(average_grads)

    init = tf.global_variables_initializer()

    gpu_options = tf.GPUOptions(allow_growth=True)
    # with tf.Session(config=tf.ConfigProto(gpu_options=gpu_options, device_count={'GPU': 2})) as sess:
    with tf.Session(config=tf.ConfigProto(allow_soft_placement=True, log_device_placement=True, gpu_options=gpu_options)) as sess:

      sess.run(tf.global_variables_initializer())
      sess.run(tf.local_variables_initializer())
      sess.run(init)

      epoch2display = 2
      start_time = time.time()
      for epoch in range(10):

        random.shuffle(train_set)

        epoch_total_loss, cnt = 0.0, 0
        for _, uij in DataInput(train_set, train_batch_size*gpu_num):

          loss_, _ = sess.run([loss_op, train_op], feed_dict={
                  i_batch: uij[1],
                  j_batch: uij[2],
                  y_batch: uij[2],
                  hist_i_batch: uij[3],
                  sl_batch: uij[4]
              })

          epoch_total_loss += loss_
          cnt += 1
          epoch_mean_loss = epoch_total_loss / float(cnt)

        if (epoch>0) and ((epoch+1 % epoch2display==0) or (epoch==9)):
            auc_train = _eval_logdata_improve(sess, train_set, train_batch_size*gpu_num, 'train')
            auc_test = _eval_logdata_improve(sess, test_set_with_label, test_batch_size*gpu_num, 'test')
            print(f'Epoch {epoch} DONE\tCost time: {time.time()-start_time}\ttrain epoch average loss: {epoch_mean_loss}\ttrain auc: {auc_train}\ttest auc: {auc_test}')
            sys.stdout.flush()

      print('best train_gauc %.4f\ttest_gauc: %.4f' % (best_auc_train, best_auc_test))
      sys.stdout.flush()
