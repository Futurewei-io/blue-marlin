# Reading CSV content from a file
import csv
import os
import pandas as pd
import random
import pickle

random.seed(1234)

def read_files(csv_location, columns_name):
    names = []
    # csv_location = '/Users/fvaseghi/Desktop/persona416'

    for file in os.listdir(csv_location):
        if file.startswith("part"):
            names.append(file)
    file_paths = [os.path.join(csv_location, name) for name in names]
    li = []
    for filename in file_paths:
        df = pd.read_csv(filename, names=columns_name )
        li.append(df)
    frame = pd.concat(li, ignore_index=True)
    return frame

def build_map(df, col_name):
  key = sorted(df[col_name].unique().tolist())
  m = dict(zip(key, range(len(key))))
  df[col_name] = df[col_name].map(lambda x: m[x])
  return m, key




persona_location = '/home/faezeh/Desktop/persona416'
persona_name =["did", "gender", "age", "price", "daily_installed",
                                                 "day30_install", "app_intrest", "province", "active_user", "active_user_30days", "pt_d"]
persona = read_files(csv_location=persona_location, columns_name=persona_name)

keyword_location='/home/faezeh/Desktop/keyword421'
keyword_name = ["keyword", "adv_id", "pt_d"]
keyword=read_files(csv_location=keyword_location , columns_name=keyword_name)

keyword_dict = {}
for adv_id, hist in keyword.groupby('adv_id'):
    df = hist['keyword'].tolist()
    keyword_dict.update({adv_id:df})

imp_location = '/home/faezeh/Desktop/impression430'
impression_name = ['log_id', 'record_time_msec', 'adv_id', 'media_busin_income_amt',
          'adv_type', 'mobile_brand', 'slot_id', 'imei', 'net_type', 'did', 'click', 'pt_d']
impression=read_files(imp_location, impression_name).sort_values(['record_time_msec']).reset_index()

# asin_map, asin_key = build_map(persona, 'asin')
# cate_map, cate_key = build_map(meta_df, 'categories')
# revi_map, revi_key = build_map(reviews_df, 'reviewerID')

#item_count : number of item that has reviews out of all items in electonics in amazon
#cate_count : number of category


impression_df = impression[['log_id', 'did', 'record_time_msec']]
# cate_list = impression[impression['click']==1]['adv_id'].values
# user_count,item_count, cate_count, example_count = impression['did'].nunique(), impression[impression['click']==1]['did'].count(), impression['adv_id'].nunique(),impression[impression['click']==1]['log_id'].count()
cate_list = list(keyword_dict.keys())
cat = set()
for val in keyword_dict.values():
    cat.add(str(val))
user_count, item_count, cate_count, example_count = impression['did'].nunique(), keyword['adv_id'].nunique(), len(cat), impression[impression['click'] == 1]['log_id'].count()

train_set = []
test_set = []

for did, hist in impression.groupby('did'):
    pos_list = hist[hist['click']==1]['adv_id'].tolist()
    neg_list = hist[hist['click']==0]['adv_id'].tolist()

    if len(pos_list) <= len(neg_list):
        length = len(pos_list)
    else:
        length = len(neg_list)
    if length > 1:

        for i in range(1, length):
            hist = pos_list[:i]
            if i != length - 1:
                train_set.append((did, hist, pos_list[i], 1))
            else:
                pos = pos_list[i]
        for i in range(1, length):
            hist = neg_list[:i]
            if i != length -1:
                train_set.append((did, hist, neg_list[i], 0))
            else:
                neg = neg_list[i]
        label = (pos, neg)
        test_set.append((did, hist, label))


# for did, hist in impression[impression['click']==1].groupby('did'):
#   pos_list = hist['adv_id'].tolist()
#
#   for i in range(1, len(pos_list)):
#     hist = pos_list[:i]
#     if i != len(pos_list) - 1:
#         train_set.append((did, hist, pos_list[i], 1))
#     else:
#         test_set.append((did, hist, pos_list[i], 1 ))
# count = len(train_set)
# for did, hist in impression[impression['click']==0].groupby('did'):
#   neg_list = hist['adv_id'].tolist()
#   if len(train_set) <= (2*count):
#       for i in range(1, len(neg_list)):
#         hist = neg_list[:i]
#         if i != len(neg_list) - 1:
#             train_set.append((did, hist, neg_list[i], 0))
#         else:
#             test_set.append((did, hist, neg_list[i], 0))
#   else:
#       assert "Done!"

print(len(train_set))
print(len(test_set))

random.shuffle(train_set)
random.shuffle(test_set)



with open('dataset.pkl', 'wb') as f:
  pickle.dump(train_set, f, pickle.HIGHEST_PROTOCOL)
  pickle.dump(test_set, f, pickle.HIGHEST_PROTOCOL)
  pickle.dump(cate_list, f, pickle.HIGHEST_PROTOCOL)
  pickle.dump((user_count, item_count, cate_count), f, pickle.HIGHEST_PROTOCOL)


