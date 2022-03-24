import pandas as pd
import numpy as np
import pickle
import random
from tqdm import tqdm
from tensorflow.keras.preprocessing.sequence import pad_sequences


def sparseFeature(feat, feat_num, embed_dim=4):
    """
    create dictionary for sparse feature
    :param feat: feature name
    :param feat_num: the total number of sparse features that do not repeat
    :param embed_dim: embedding dimension
    :return:
    """
    return {'feat': feat, 'feat_num': feat_num, 'embed_dim': embed_dim}


def denseFeature(feat):
    """
    create dictionary for dense feature
    :param feat: dense feature name
    :return:
    """
    return {'feat': feat}


def tf1_to_tf2_data_conversion(data_file_tf1, lbl_file_tf1, embed_dim=8):

    print('==========Data conversion start============')
    with open(data_file_tf1, 'rb') as f:
        train_set_tf1 = pickle.load(f)
        test_set_tf1 = pickle.load(f)
        _ = pickle.load(f)
        _, item_count, cate_count = pickle.load(f)

    maxlen = max([len(sample[1]) for sample in train_set_tf1])

    train_data = []
    for sample_tf1 in train_set_tf1:
        if len(sample_tf1[1])<10:
            continue
        h = [[item, item] for item in sample_tf1[1]]
        c = [sample_tf1[2], sample_tf1[2]]
        l = sample_tf1[3]
        train_data.append([h, c, l])

    with open(lbl_file_tf1, 'rb') as f:
        test_lb = pickle.load(f)
        _ = pickle.load(f)

    test_data = []
    for i, sample_tf1 in enumerate(test_set_tf1):
        if len(sample_tf1[1])<10:
            continue
        h = [[item, item] for item in sample_tf1[1]]
        c = list(sample_tf1[2])
        l = test_lb[i]
        test_data.append([h, c, l])

    # feature columns
    feature_columns = [[],
                       # [sparseFeature('item_id', item_count, embed_dim),
                       [sparseFeature('item_id', item_count+1, embed_dim),
                        ]]  # sparseFeature('cate_id', cate_count, embed_dim)

    # behavior
    behavior_list = ['item_id']  # , 'cate_id'

    # shuffle
    random.shuffle(train_data)
    random.shuffle(test_data)

    # create dataframe
    train = pd.DataFrame(train_data, columns=['hist', 'target_item', 'label'])
    test = pd.DataFrame(test_data, columns=['hist', 'target_item', 'label'])

    # if no dense or sparse features, can fill with 0
    print('==================Padding===================')
    train_X = [np.array([0.] * len(train)), np.array([0] * len(train)),
               pad_sequences(train['hist'], maxlen=maxlen),
               np.array(train['target_item'].tolist())]
    train_y = train['label'].values
    test_X = [np.array([0] * len(test)), np.array([0] * len(test)),
              pad_sequences(test['hist'], maxlen=maxlen),
              np.array(test['target_item'].tolist())]
    test_y = test['label'].values
    print('============Data conversion end=============')
    return feature_columns, behavior_list, (train_X, train_y), (test_X, test_y)
