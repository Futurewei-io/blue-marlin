#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
 
#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# testing datasets for din performance forecasting.
gucdoc_columns = ['region_id', 'age', 'gender', 'net_type', 'media_category', 'media',
                  'uckey', 'interval_starting_time', 'keyword_indexes', 'keyword_indexes_click_counts',
                  'keyword_indexes_show_counts', 'keywords', 'keywords_click_counts', 'keywords_show_counts',
                  'uckey_index', 'media_index', 'media_category_index', 'net_type_index', 'gender_index',
                  'age_index', 'region_id_index']
gucdoc_tested = [
    (26, 1, 0, '4G', 'Honor Reading', 'splash', 'splash,Honor Reading,4G,0,1,26',
     [1578009600], ['25'], ['25:0'], ['25:1'], ['shopping'], ['shopping:0'],
     ['shopping:1'], 18565, 3, 2, 3, 1, 1, 26),
    (26, 1, 0, '4G', 'Huawei Reading', 'splash', 'splash,Huawei Reading,4G,0,1,26',
     [1581552000], ['25'], ['25:0'], ['25:1'], ['shopping'], ['shopping:0'],
     ['shopping:1'], 22815, 3, 5, 3, 1, 1, 26),
    (26, 1, 0, 'WIFI', 'Huawei Music', 'splash', 'splash,Huawei Music,WIFI,0,1,26',
     [1586908800, 1583798400, 1583712000, 1580515200, 1579737600, 1579651200],
     ['29', '29', '29', '29', '29', '25'],
     ['29:0', '29:0', '29:0', '29:0', '29:0', '25:0'],
     ['29:1', '29:1', '29:1', '29:1', '29:2', '25:1'],
     ['video', 'video', 'video', 'video', 'video', 'shopping'],
     ['video:0', 'video:0', 'video:0', 'video:0', 'video:0', 'shopping:0'],
     ['video:1', 'video:1', 'video:1', 'video:1', 'video:2', 'shopping:1'],
     21475, 3, 4, 5, 1, 1, 26)
]
keyword_columns = ['keyword', 'spread_app_id', 'keyword_index']
keyword_tested = [
    ('video', 'C10097595', 29),
    ('shopping', 'C100709343', 25)
]
