from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql import SparkSession
from pyspark.ml.feature import Normalizer
import time

numHashTables=5

spark = SparkSession \
    .builder \
    .getOrCreate()

df = spark.read.parquet("user_score")
normalizer = Normalizer(inputCol="user_score", outputCol="normFeatures", p=2.0)
extended_user_df = normalizer.transform(df)
extended_user_df.cache()
seed_user_df = extended_user_df.sample(0.1, False)

print("no seed users: ",seed_user_df.count(),"   no of extended users:  ",extended_user_df.count())

# LSH Algorithm
start_time=time.time()
brp = BucketedRandomProjectionLSH(inputCol="normFeatures", outputCol="hashes", bucketLength=10000.0,numHashTables=numHashTables)
model = brp.fit(extended_user_df)
df_users=model.approxSimilarityJoin(seed_user_df, extended_user_df, 0.99, distCol="EuclideanDistance")
df_users.coalesce(100).write.mode('overwrite').parquet("user_similarity")
print("{} seconds time take by the script:  ".format(time.time()-start_time))

df = spark.read.parquet("user_similarity")
df.orderBy("EuclideanDistance", ascending=False).show(truncate=False)