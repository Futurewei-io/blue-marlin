import numpy as np
import faiss

'''
TO install faiss python lib 
    command :  pip3 install faiss-gpu 
'''


# RANDOMLY GENRATED DATA
dim_of_user_vector = 64
num_user = 30000000
np.random.seed(1234)  # make reproducible
xb = np.random.random((num_user, dim_of_user_vector)).astype('float32')
xb[:, 0] += np.arange(num_user) / 1000.


# faiss logic part
no_gpus = faiss.get_num_gpus()
print("num of gpus available count:  ", no_gpus)
cpu_index = faiss.IndexFlatL2(dim_of_user_vector)
gpu_index = faiss.index_cpu_to_all_gpus(cpu_index)
gpu_index.add(xb)

print("Total num of records indexed: ", gpu_index.ntotal)

k = 11  # we want to see 4 nearest neighbors
DISTANCE, K_NEAREST_NEIGHBOR = gpu_index.search(xb, k)

print(DISTANCE[:10])
print(K_NEAREST_NEIGHBOR[:10])

