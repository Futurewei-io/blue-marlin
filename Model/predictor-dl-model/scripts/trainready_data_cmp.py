import pyspark.sql.functions as fn

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import udf, lit, col, expr, collect_list, avg, rand, dense_rank
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType, BooleanType
from pyspark.sql.window import Window
from pyspark.sql import HiveContext

import numpy as np


def get_diff(_c0):
    def change(_):
        try:
            return(int(float(_)))
        except:
            return 0

    r = reza_dict[_c0]['ts']
    j = jimmy_dict[_c0]['ts']
    j = [change(_) for _ in j]

    if (len(r) != len(j)):
        print('WARNING {} {}'.format(str(len(r)), str(len(j))))

    return np.array(r)-np.array(j[:len(r)])


# spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 32G --conf spark.driver.maxResultSize=10G trainready_data_cmp.py
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
hive_context = HiveContext(sc)

df = hive_context.read.format('com.databricks.spark.csv').options(header='true').load('train_ready_bad_uckey_removal_10percent_denoise.csv')
c = df.columns
del c[0]
df = df.withColumn('ts', udf(lambda x: [x[_] for _ in c], ArrayType(StringType()))(fn.struct(c)))
df = df.withColumn('sparse', udf(lambda x: 'vir' in x, BooleanType())(col('_c0')))
df = df.filter('sparse==False')
df = df.withColumn('imp', udf(lambda x: int(sum([float(_) for _ in x if _])), IntegerType())(df.ts))
df = df.select('_c0', 'ts', 'sparse', 'imp')
# df.show(1, False)
jimmy = df.collect()
jimmy_dict = {}
for _ in jimmy:
    jimmy_dict[_['_c0']] = _


df = hive_context.sql('select * from dlpm_06242021_1635_trainready')
df = df.withColumn('sparse', udf(lambda x: ',' not in x, BooleanType())(col('uckey')))
df = df.filter('sparse==False')
df = df.withColumn('imp', udf(lambda x: int(sum([float(_) for _ in x if _])), IntegerType())(df.ts))
df = df.withColumn('_c0', udf(lambda x, y: x+','+str(y), StringType())(df.uckey, df.price_cat))
df = df.select('_c0', 'ts', 'sparse', 'imp')
# df.show(1, False)
reza = df.collect()
reza_dict = {}
for _ in reza:
    reza_dict[_['_c0']] = _


imp_diff_list = []
for r in reza:
    _c0 = r['_c0']
    if _c0 in jimmy_dict:
        j = jimmy_dict[_c0]
        imp_diff = r['imp']-j['imp']
        imp_diff_list.append((_c0, imp_diff))

imp_diff_list = sorted(imp_diff_list, key=lambda x: x[1])


if (False):
    for _ in imp_diff_list[-5:]:
        print(get_diff(_[0]))

    for _ in imp_diff_list[:5]:
        print(get_diff(_[0]))

    with open("result-cmp.txt", "w") as f:
        for _ in imp_diff_list:
            r = get_diff(_[0])
            f.write(str(r))

if (True):
    index_of_nonzero_set = set()
    counter = {}
    for _ in imp_diff_list:
        r = get_diff(_[0])
        non_zero = np.count_nonzero(r)
        if (non_zero) > 8:
            print(r)
        
        if non_zero not in counter:
            counter[non_zero] = 0
        counter[non_zero] += 1

        if non_zero == 1:
            index_of_nonzero = np.nonzero(r)
            index_of_nonzero_set.add(index_of_nonzero[0][0])


    print(counter)
    print(index_of_nonzero)


print('DENSE only analysis')
print('jimmy size: '+str(len(jimmy)))
print('reza size: '+str(len(reza)))
print('diff: '+str(abs(len(jimmy)-len(reza))))
print('common: '+str(len(imp_diff_list)))
