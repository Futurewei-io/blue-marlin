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
from datetime import datetime
from scipy.spatial.distance import cdist

def naive_distance(a, b):
    cross_product = np.matmul(a, b.transpose())
    c1 = np.sum(np.square(a), 1)
    c2 = np.sum(np.square(b), 1)
    m = a.shape[1]
    similarity = np.sqrt(m) - np.sqrt(np.maximum(np.expand_dims(c1, 1) + c2 - (2 * cross_product), 0.0))
    #distance = np.sqrt(max(0, (-2 * cross_product) + c1[None,:] + c2[:,None]))
    #print('distance = \n{}'.format(distance))
    # similarity = np.sqrt(m) - distance
    return similarity

def alt_cosine(x,y):
    return 1 - np.inner(x,y)/np.sqrt(np.dot(x,x)*np.dot(y,y))
    # inner = np.inner(x,y)
    # x_dot = np.dot(x,x)
    # y_dot = np.dot(y,y)
    # denom = np.sqrt(x_dot*y_dot)
    # result = 1 - inner/denom
    # return result

def alt_cosine_2(x,y):
    return 1 - np.inner(x,y)/np.sqrt(np.expand_dims(np.sum(np.square(x),1), 1)*np.sum(np.square(y),1))

# from libc.math cimport sqrt
# def alt_cosine_3(x,y):
#     return 1 - np.inner(x,y)/sqrt(np.dot(x,x)*np.dot(y,y))

# from libc.math cimport sqrt
# import numpy as np
# cimport numpy as np

# def cy_cosine(np.ndarray[np.float64_t] x, np.ndarray[np.float64_t] y):
#     cdef double xx=0.0
#     cdef double yy=0.0
#     cdef double xy=0.0
#     cdef Py_ssize_t i
#     for i in range(len(x)):
#         xx+=x[i]*x[i]
#         yy+=y[i]*y[i]
#         xy+=x[i]*y[i]
#     return 1.0-xy/sqrt(xx*yy)

# import numba as nb
# import numpy as np
# @nb.jit(nopython=True, fastmath=True)
# def nb_cosine(x, y):
#     xx,yy,xy=0.0,0.0,0.0
#     for i in range(len(x)):
#         xx+=x[i]*x[i]
#         yy+=y[i]*y[i]
#         xy+=x[i]*y[i]
#     return 1.0-xy/np.sqrt(xx*yy)




num_features = 10
num_rows_a = 10000
num_rows_b = 1000
num_rounds = 1

start = datetime.now()

a = []
for _ in range(num_rows_a):
    a.append(np.random.rand(num_features))
am = np.random.rand(num_rows_a, num_features)

b = []
for _ in range(num_rows_b):
    b.append(np.random.rand(num_features))
bm = np.random.rand(num_rows_b, num_features)

s = datetime.now()
naive_distance(am, bm)
print('Current: {}'.format(datetime.now() - start))

s = datetime.now()
cdist(am, bm)
print('cdist: {}'.format(datetime.now() - s))

# s = datetime.now()
# for i in a:
#     for j in b:
#         alt_cosine(i, j)
# print('{}'.format(datetime.now() - s))

s = datetime.now()
alt_cosine_2(am, bm)
print('alt_cosine: {}'.format(datetime.now() - s))

# s = datetime.now()
# for i in a:
#     for j in b:
#         nb_cosine(i, j)
# print('numba: {}'.format(datetime.now() - s))

print('Done')

