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


import numpy as np
import faiss
import time
import datetime

def testIndex(index, n, num_user, num_user_step, search_step):
    name = str(type(index)).split('.')[-1].split('\'')[0]
    print('Testing {}'.format(name))

    start = time.time()
    gpu_index = faiss.index_cpu_to_all_gpus(index)

    for i in range(0, num_user, num_user_step):
        xb = np.random.random((num_user_step, dim_of_user_vector)).astype('float32')
        # xb[:, 0] += np.arange(num_user) / 1000.
        # print(xb.shape)

        if not gpu_index.is_trained:
            gpu_index.train(xb)
            print('{}: {} training: {}'.format(i, name, time.time() - start))
        else:
            print('{}: No training needed.'.format(i))

        gpu_index.add(xb)
    end_load = time.time()

    total_search_time = 0
    for i in range(0, num_user, search_step):
        xb = np.random.random((num_user_step, dim_of_user_vector)).astype('float32')
        print(i)

        # search top 10 users which are similar to 1M query users
        # Function gpu_index.search() return top 10 user index and its Distance from 1M queried users
        search_start = time.time()
        DISTANCE, K_NEAREST_NEIGHBOR = gpu_index.search(xb, n)
        total_search_time += time.time()-search_start

    print('  {} test: {}'.format(name, str(datetime.timedelta(seconds=time.time() - start))))
    print('  Load index time:  ', str(datetime.timedelta(seconds=end_load - start)))
    print('  Total search time:', str(datetime.timedelta(seconds=total_search_time)))
    print('  nprobe =', index.nprobe)

# Generate the test dataset.
dim_of_user_vector = 32
n             = 10
nprobe        = 1
num_user      = 10000000
num_user_step = 500000
search_step   = 1000000
np.random.seed(1234)

# Query the number of GPU resources.
no_gpus = faiss.get_num_gpus()
print('num of gpus available count: {}'.format(no_gpus))

# Test different index subclasses.
index = faiss.index_factory(dim_of_user_vector, "IVF1024,Flat")
index.nprobe = nprobe
testIndex(index, n, num_user, num_user_step, search_step)
# testIndex(faiss.IndexLSH(dim_of_user_vector, 10))



