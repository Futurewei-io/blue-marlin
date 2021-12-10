import os
import tensorflow as tf
import tensorflow_datasets as tfds
from tensorboard.plugins import projector

#  C:\Users\a00545515\Anaconda3\Scripts\tensorboard.exe --logdir=G:/tf_log/deepfm_projector

# log_dir='G:/tf_log/deepfm_projector'
# model_path= "G:/tf_log/deepfmm"
# python3 /home/antpc/.local/lib/python3.6/site-packages/tensorboard/main.py --host 0.0.0.0 --logdir /home/antpc/14/tf_log/deepfm_projector
log_dir = "/home/antpc/14/tf_log/deepfm_projector"
model_path = "/home/antpc/14/deep_fm_model"

num_users=4096

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

encoder=[str(i) for i in range(num_users)]


with open(os.path.join(log_dir, 'metadata.tsv'), "w") as f:
  for subwords in encoder:
    f.write("{}\n".format(subwords))



model=tf.keras.models.load_model(model_path)
# model.summary()

uid_id=model.get_layer("user")
weights = tf.Variable(uid_id.get_weights()[0])

checkpoint = tf.train.Checkpoint(embedding=weights)
checkpoint.save(os.path.join(log_dir, "embedding.ckpt"))

config = projector.ProjectorConfig()
embedding = config.embeddings.add()
embedding.tensor_name = "embedding/.ATTRIBUTES/VARIABLE_VALUE"
embedding.metadata_path = 'metadata.tsv'
projector.visualize_embeddings(log_dir, config)