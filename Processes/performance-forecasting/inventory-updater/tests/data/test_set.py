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

# testing datasets for din performance forecasting.
gucdoc_columns = ['media_category', 'region_id', 'age', 'gender', 'net_type', 'media',
                  'uckey', 'interval_starting_time', 'keyword_indexes', 'keyword_indexes_click_counts',
                  'keyword_indexes_show_counts', 'keywords', 'keywords_click_counts', 'keywords_show_counts',
                  'uckey_index', 'media_index', 'media_category_index', 'net_type_index', 'gender_index',
                  'age_index', 'region_id_index',
                  'travel', 'game-avg', 'game-ent', 'reading', 'living-food', 'education', 'living-car',
                  'shopping', 'living-house', 'game-fishing', 'game-mon', 'other', 'living-map', 'game-act',
                  'game-slg', 'social', 'living-makeup', 'health', 'game-cnc', 'game-sim', 'video', 'game-rpg',
                  'living-mon', 'sports', 'living-photo', 'entertainment', 'living-insurance', 'info',
                  'game-moba', 'slot_ids']
gucdoc_tested = [
    ('AI assistant', 39, 6, 1, 'WIFI', 'native',
     'native,AI assistant,WIFI,1,6,39',
     [1586908800, 1586822400, 1586736000, 1586476800, 1586044800, 1585872000, 1585699200,
      1585526400, 1585440000, 1585267200, 1585180800, 1585008000, 1584662400, 1584576000,
      1584230400, 1584144000, 1582502400, 1582329600, 1582243200, 1582070400, 1581984000,
      1581811200, 1581724800, 1581552000, 1581379200, 1581292800, 1581206400, 1581120000,
      1580860800, 1580688000, 1580601600, 1580515200, 1580428800, 1580342400, 1580169600,
      1580083200, 1579996800, 1579910400, 1579824000, 1579392000, 1577923200, 1577145600,
      1576886400],
     ['29', '29', '29', '29', '29', '29', '29', '29', '29', '25', '25', '25', '25', '25',
      '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25',
      '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25', '25'],
     ['29:0', '29:0', '29:0', '29:0', '29:0', '29:0', '29:0', '29:0', '29:0', '25:0', '25:0',
      '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0',
      '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0',
      '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0', '25:0'],
     ['29:2', '29:2', '29:1', '29:1', '29:1', '29:2', '29:2', '29:1', '29:1', '25:1', '25:2',
      '25:4', '25:1', '25:1', '25:2', '25:2', '25:2', '25:1', '25:1', '25:1', '25:1', '25:2',
      '25:1', '25:2', '25:1', '25:1', '25:3', '25:3', '25:2', '25:1', '25:1', '25:2', '25:1',
      '25:1', '25:1', '25:1', '25:2', '25:3', '25:1', '25:1', '25:2', '25:5', '25:1'],
     ['video', 'video', 'video', 'video', 'video', 'video', 'video', 'video', 'video',
      'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping',
      'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping',
      'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping',
      'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping',
      'shopping', 'shopping', 'shopping', 'shopping', 'shopping', 'shopping'],
     ['video:0', 'video:0', 'video:0', 'video:0', 'video:0', 'video:0', 'video:0', 'video:0',
      'video:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0',
      'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0',
      'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0',
      'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0',
      'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0', 'shopping:0'],
     ['video:2', 'video:2', 'video:1', 'video:1', 'video:1', 'video:2', 'video:2', 'video:1', 'video:1',
      'shopping:1', 'shopping:2', 'shopping:4', 'shopping:1', 'shopping:1', 'shopping:2', 'shopping:2',
      'shopping:2', 'shopping:1', 'shopping:1', 'shopping:1', 'shopping:1', 'shopping:2', 'shopping:1',
      'shopping:2', 'shopping:1', 'shopping:1', 'shopping:3', 'shopping:3', 'shopping:2', 'shopping:1',
      'shopping:1', 'shopping:2', 'shopping:1', 'shopping:1', 'shopping:1', 'shopping:1', 'shopping:2',
      'shopping:3', 'shopping:1', 'shopping:1', 'shopping:2', 'shopping:5', 'shopping:1'],
     2858, 1, 1, 5, 2, 6, 39,
     0.03720402717590332, 0.041365355253219604, 0.044269680976867676, 0.027474403381347656, 0.042772114276885986,
     0.026846081018447876, 0.042723000049591064, 0.05180484056472778, 0.033589571714401245, 0.03995606303215027,
     0.04154631495475769, 0.04310709238052368, 0.03218933939933777, 0.04509803652763367, 0.04074868559837341,
     0.05415108799934387, 0.024545997381210327, 0.04443544149398804, 0.04553502798080444, 0.04123121500015259,
     0.06090787053108215, 0.036785125732421875, 0.02137276530265808, 0.04195219278335571, 0.04461422562599182,
     0.02520167827606201, 0.03915587067604065, 0.05557537078857422, 0.04309225082397461,
     ['71bcd2720e5011e79bc8fa163e05184e', 'a47eavw7ex', '68bcd2720e5011e79bc8fa163e05184e',
      '66bcd2720e5011e79bc8fa163e05184e']
     )
]
