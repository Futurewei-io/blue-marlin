import tensorflow as tf
from time import time
from tensorflow.keras.losses import binary_crossentropy
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.metrics import AUC

from model_tf2 import DIN
from utils_tf2 import *

import os
import pickle

from tensorflow.keras.utils import plot_model
from IPython.display import Image

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ['CUDA_VISIBLE_DEVICES'] = '0,1,2,3,4,5,6,7'

if __name__ == '__main__':
    # ========================= Hyper Parameters =======================
    embed_dim = 8
    att_hidden_units = [80, 40]
    ffn_hidden_units = [256, 128, 64]
    dnn_dropout = 0.5
    att_activation = 'sigmoid'
    ffn_activation = 'prelu'

    learning_rate = 0.001
    batch_size = 4096
    epochs = 50

    data_file_tf1 = 'raw_data/ad_dataset_lookalike_11192021_10_faezeh.2compare.sort.pkl'
    lbl_file_tf1 = 'raw_data/label_lookalike_11192021_10_faezeh.2compare.sort.pkl'
    if not os.path.exists('raw_data/train_test_lookalike.pkl'):
        feature_columns, behavior_list, train, test, maxlen = tf1_to_tf2_data_conversion(data_file_tf1, lbl_file_tf1, embed_dim)
        with open('raw_data/train_test_lookalike.pkl', 'wb') as f:
            pickle.dump(feature_columns, f, pickle.HIGHEST_PROTOCOL)
            pickle.dump(behavior_list, f, pickle.HIGHEST_PROTOCOL)
            pickle.dump(train, f, pickle.HIGHEST_PROTOCOL)
            pickle.dump(test, f, pickle.HIGHEST_PROTOCOL)
    else:
        with open('raw_data/train_test_lookalike.pkl', 'rb') as f:
            feature_columns = pickle.load(f)
            behavior_list = pickle.load(f)
            train = pickle.load(f)
            test = pickle.load(f)


    train_X, train_y = train
    # val_X, val_y = val
    test_X, test_y = test
    s = time()
    # ============================Build Model==========================
    maxlen = train_X[2].shape[1]
    print(f'maxlen: {maxlen}')
    model = DIN(feature_columns, behavior_list, att_hidden_units, ffn_hidden_units, att_activation,
        ffn_activation, maxlen, dnn_dropout)
    model.summary()
    # ============================model checkpoint======================
    # check_path = 'save/din_weights.epoch_{epoch:04d}.val_loss_{val_loss:.4f}.ckpt'
    # checkpoint = tf.keras.callbacks.ModelCheckpoint(check_path, save_weights_only=True,
    #                                                 verbose=1, period=5)
    # =========================Compile============================
    model.compile(loss=binary_crossentropy, optimizer=Adam(learning_rate=learning_rate),
                  metrics=[AUC()])

    # ===========================Fit==============================
    model.fit(
        train_X,
        train_y,
        epochs=epochs,
        # callbacks=[EarlyStopping(monitor='val_loss', patience=2, restore_best_weights=True)],  # checkpoint
        # validation_data=(val_X, val_y),
        validation_data=(test_X, test_y),
        batch_size=batch_size,
    )

    # ===========================Test==============================
    print('test AUC: %f' % model.evaluate(test_X, test_y, batch_size=batch_size)[1])
    print(f'elapse: {time()-s} secs')