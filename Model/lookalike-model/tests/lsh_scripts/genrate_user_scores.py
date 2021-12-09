from pyspark.sql import SQLContext,SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import VectorAssembler, VectorIndexer, StringIndexer
from pyspark.sql.functions import count, lit, col, udf, expr, collect_list, explode
from pyspark.sql.types import *
from pyspark.mllib.random import RandomRDDs
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.random import RandomRDDs


num_users=100000

user_embedding_dim=20
spark = SparkSession \
    .builder \
    .getOrCreate()

def generate_random_uniform_df(nrows, ncols):
    df  = RandomRDDs.uniformVectorRDD(spark.sparkContext, nrows,ncols).map(lambda a : a.tolist()).toDF()
    return df

df=generate_random_uniform_df(num_users,20)
vectorAssembler = VectorAssembler().setInputCols(df.columns).setOutputCol("user_score")
ddf = vectorAssembler.transform(df).select("user_score")
ddf.coalesce(100).write.mode('overwrite').parquet("user_score")

