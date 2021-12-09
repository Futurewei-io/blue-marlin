import tensorflow as tf
from deep_fm import DeepFM,TF_Record_reader
import numpy as np
import pickle
import argparse

class UserEmbeddingModel(tf.keras.Model):
    def __init__(self, model_save_path,user_id_col):
        super(UserEmbeddingModel, self).__init__()
        self.model=tf.keras.models.load_model(filepath=model_save_path).get_layer(user_id_col)
        self.model.trainable=False
    def call(self, inputs, training=None, mask=None):
        user_embeddings=self.model(inputs)
        user_embeddings = tf.squeeze(user_embeddings, axis=1)
        l2norm = tf.sqrt(tf.reduce_sum(tf.square(user_embeddings), axis=1, keepdims=True))
        l2norm_user_embeddings = user_embeddings * (1 / l2norm)
        return l2norm_user_embeddings

if __name__ == "__main__":
    print("job is started")
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, help='pickle_file')
    parser.add_argument("--user_emb_file_path", type=str, help='pickle_file')
    parser.add_argument("--model_save_path", type=str, help='pickle_file')

    args, unknown = parser.parse_known_args()

    user_emb_file_path=args.user_emb_file_path
    model_save_path=args.model_save_path

    pickel_file_path = args.data_dir + "/lookalike.pkl"

    with open(pickel_file_path, 'rb') as f:
        hyperparam = pickle.load(f)

    user_id_col = hyperparam["DATA_PREP"]["user_id_col"]
    no_unique_users=hyperparam["MODEL"]["DEEP_FM_FEATURES"][user_id_col]["unique_count"]
    model = UserEmbeddingModel(model_save_path,user_id_col)
    user_ids = tf.constant([i for i in range(no_unique_users)],shape=(no_unique_users,1))
    l2norm_user_embeddings=model.predict(user_ids)
    np.save(user_emb_file_path, l2norm_user_embeddings)

