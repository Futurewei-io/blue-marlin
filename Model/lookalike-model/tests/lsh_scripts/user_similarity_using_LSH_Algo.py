from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.feature import Normalizer
import random
import time

numHashTables=5

spark = SparkSession \
    .builder \
    .getOrCreate()

df = spark.read.parquet("user_score")
normalizer = Normalizer(inputCol="user_score", outputCol="normFeatures", p=2.0)
extended_user_df = normalizer.transform(df)
extended_user_df.cache()
# seed_user_df = extended_user_df.sample(0.1, False)

# print("no seed users: ",seed_user_df.count(),"   no of extended users:  ",extended_user_df.count())

# LSH Algorithm
start_time=time.time()
brp = BucketedRandomProjectionLSH(inputCol="normFeatures", outputCol="hashes", bucketLength=10000.0, numHashTables=numHashTables)
brp.setSeed(random.randint())
model = brp.fit(extended_user_df)

# Get the hashes for the users and convert them into a cluster ID number.
df_users = model.transform(extended_user_df)
df_users = df_users.withColumn('cluster_id', udf(lambda input: reduce(lambda x, y: x | y, [ 0x1 << i if value[0] != 0.0 else 0 for i, value in enumerate(input) ]), IntegerType())(df_users.hashes))
#df_users.select('hashes', 'cluster_id').show(50, truncate=False, vertical=True)
df_count = df_users.groupBy(['cluster_id', 'hashes']).count().cache()
df_count.show(100, truncate=False)
df_count.groupBy().max('count').show()

# df_users = model.approxSimilarityJoin(seed_user_df, extended_user_df, 0.99, distCol="EuclideanDistance")
# df_users.coalesce(100).write.mode('overwrite').parquet("user_similarity")
print("{} seconds time take by the script:  ".format(time.time()-start_time))

# df = spark.read.parquet("user_similarity")
# df.orderBy("EuclideanDistance", ascending=False).show(truncate=False)