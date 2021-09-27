import numpy as np
import time

N = 1000
a = np.random.randint(100, size=(N, N))
b = np.random.randint(100, size=(N, N))

result = []
start = time.time()
for i in range(len(a)):
    c = a[i].dot(b)
    result.append(c)
print(time.time()-start)


start = time.time()
c = a.dot(b)
print(time.time()-start)

'''
The results shows that [vector,...] by matrix takes almost the same time as matrix by matri, on signle machine.
'''