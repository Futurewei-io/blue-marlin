import numpy as np
import os, time
import random
import tensorflow as tf
from lookalike_model.trainer.model_new import Model
import argparse


random.seed(1234)
# adding arguments for tfrecord directory and the checkpoint directory
parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, help="input data tfrecords dir location")
parser.add_argument("--check_point_dir", type=str, help="Check Point dir location")
args, unknown = parser.parse_known_args()
if len(unknown) != 0:
    print("unknown args:%s", unknown)

# tfrecord location and the check point directory location
tfrecord_location =args.data_dir + "/tf_records_lookalike_data_08july"
output = args.check_point_dir


def __data_parser(serialized_example):
    features = tf.parse_single_example(serialized_example,
                                       features={'keywords_list': tf.FixedLenSequenceFeature([], tf.int64, allow_missing=True),
                                                 'ucdoc': tf.FixedLenFeature([], tf.int64),
                                                 'keyword': tf.FixedLenFeature([], tf.int64),
                                                 'is_click': tf.FixedLenFeature([], tf.float32),
                                                 'sl': tf.FixedLenFeature([], tf.int64),
                                                 'lr': tf.FixedLenFeature([], tf.float32)
                                                 })
    keywords_list = tf.cast(features['keywords_list'], tf.int32)
    ucdoc = tf.cast(features['ucdoc'], tf.int32)
    keyword = tf.cast(features['keyword'], tf.int32)
    is_click = tf.cast(features['is_click'], tf.float32)
    sl = tf.cast(features['sl'], tf.int32)
    lr = tf.cast(features['lr'], tf.float32)
    return ucdoc, keyword, keywords_list, is_click,sl,lr


names = []
for file in os.listdir(tfrecord_location):
    if file.startswith("part"):
        names.append(file)

file_paths = [os.path.join(tfrecord_location, name) for name in names]
dataset = tf.data.TFRecordDataset(file_paths)
shuffle_value = 2000
repeat_value = 10
batch_size = 1000
prefetch_buffer = 2000
dataset = dataset.map(__data_parser)
dataset = dataset.repeat(repeat_value).shuffle(shuffle_value).prefetch(buffer_size=prefetch_buffer).batch(batch_size)
iterator = dataset.make_one_shot_iterator()

tf_ucdoc, tf_keyword, tf_keywords_list, tf_is_click, tf_sl, tf_lr = iterator.get_next()

unique_keywords = 811
cate_list = np.array([x for x in range(unique_keywords)])
user_count = 1349500103
item_count, cate_count = unique_keywords, unique_keywords
predict_batch_size = 5000
predict_ads_num = 30
total_iterations = int((user_count * epoch)//batch_size)
print('total iterations = {}'.format(total_iterations))
max_epochs = 500

model = Model(user_count, item_count, cate_count, cate_list, predict_batch_size, predict_ads_num,tf_ucdoc,tf_keyword,tf_is_click,tf_keywords_list,tf_sl)
gpu_options = tf.GPUOptions(allow_growth=True)
with tf.Session(config=tf.ConfigProto(gpu_options=gpu_options)) as sess:
  sess.run(tf.global_variables_initializer())
  sess.run(tf.local_variables_initializer())
  start_time = time.time()
  count_epoch = 0
  last_100_loss = []
  print('shuffle = {}, epochs = {}, batch_size = {}, predict_batch_size = {}'.format(shuffle_value, epoch, batch_size, predict_batch_size))
  for i in range(max_epochs*500):
      loss, _,sl = sess.run([model.loss, model.train_op, tf_sl])
      loss = round(loss, 2)
      last_100_loss.append(loss)
      if len(last_100_loss) == 101:
        del last_100_loss[0]
      if i%500==0:
            print('Epoch {} DONE Iteration: {}  Cost time: {}  Model Loss: {}  Average Loss: {}'.format(count_epoch, i, time.time()-start_time, loss,
                                                                                                        round(sum(last_100_loss)/100, 2)))
            model.save(sess, output)
            count_epoch += 1
#         print("i:  ",i,"  loss:  ",loss)
  model.save(sess, output)




