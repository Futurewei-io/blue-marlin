import yaml
import argparse
import pyspark.sql.functions as fn

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, MapType, StructType

# from rest_client import predict, str_to_intlist
import requests
import json
import argparse
from pyspark.sql.functions import udf
from math import sqrt
import time
import numpy as np
import itertools
import heapq

'''
spark-submit --master yarn --executor-memory 16G --driver-memory 24G  --num-executors 16 --executor-cores 5 --conf spark.driver.maxResultSize=8g exp_2.py
'''

if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)

    TOPN = 1000
    NUM_RECORDS = 1000
    records = []
    for i in range(NUM_RECORDS):
        v = np.random.rand(1, TOPN).tolist()[0]
        records.append((i, v))
    df1 = hive_context.createDataFrame(records, ['id', 'vector'])

    matrix = np.random.rand(TOPN, TOPN).tolist()
    matrix_broadcast = sc.broadcast(matrix)

    def _helper(vector):
        vector = np.array(vector)
        return vector.dot(matrix_broadcast.value).tolist()

    df1 = df1.withColumn('dot', udf(_helper, ArrayType(FloatType()))(df1.vector))

    start = time.time()
    df1.collect()
    print(time.time()-start)


    records = []   
    v = np.random.rand(NUM_RECORDS, TOPN).tolist()[0]
    records.append((i, v))
    df2 = hive_context.createDataFrame(records, ['id', 'matrix'])

    def _helper_2(matrix):
        matrix = np.array(matrix)
        return matrix.dot(matrix_broadcast.value).tolist()
    
    df2 = df2.withColumn('dot', udf(_helper_2, ArrayType(ArrayType(FloatType())))(df2.matrix))

    start = time.time()
    df2.collect()
    print(time.time()-start)