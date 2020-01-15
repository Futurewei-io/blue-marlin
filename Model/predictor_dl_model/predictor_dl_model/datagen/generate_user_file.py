# Copyright 2019, Futurewei Technologies
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

import random

number_if_users = 1000
gender = {'g_m':3,'g_f':5,'g_x':2}
age = {'1':1,'2':2,'3':3,'4':4}
r = {'1001':1,'1002':5,'1003':20,'1004':50,'1005':100}
activity_rate = {'h':30,'m':100,'l':10}
ai = ['finance','health','education','games','sports','fashion']

def generate_value_list(value_map):
    value_list = []
    for k,v in value_map.items():
        for i in range(0,v):
            value_list.append(k)
    return value_list

# Males with 70 have sports and games
# Females with 70 have fashion
def choice_activity_list(gender):
    random_ai = [_ for _ in ai if random.randint(1,2)%2 == 0]
    if (gender == 'g_m' and random.randint(1,10) > 3):
        random_ai.append('games')
        random_ai.append('sports')
        if ('fashion' in random_ai):
            random_ai.remove('fashion')
    if (gender == 'g_f' and random.randint(1,10) > 3):
        if ('removes' in random_ai):
            random_ai.remove('games')
        if ('sports' in random_ai):
            random_ai.remove('sports')
        random_ai.append('fashion')
    return '-'.join(set(random_ai))

if __name__ == "__main__":
    gender_list = generate_value_list(gender)
    age_list = generate_value_list(age)
    r_list = generate_value_list(r)
    activity_list = generate_value_list(activity_rate)

    users = []
    for i in range(number_if_users):
        gender = random.choice(gender_list)
        user = {'id':i,'g':gender,'a':random.choice(age_list),'r':random.choice(r_list),'activity':random.choice(activity_list),'ai':choice_activity_list(gender)}
        users.append(user)

    f = open('users.txt','w')
    f.write(str(users))
    f.close()

