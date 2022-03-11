from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, ArrayType, StringType
import pyspark.sql.functions as fn


sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)
columns = ['date', 'index', 'clicks', 'impression', 'keyword', 'aid']

# Create PySpark SparkSession
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

trainready = pd.read_csv('trainready_jimmy.csv')
df = spark.createDataFrame(trainready, columns)

df=df.withColumn('uckey',fn.udf(lambda x:x.strip(),StringType())(df.uckey))
df=df.withColumn('price_cat',df.price_cat.cast(StringType()))

df.write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").format('hive').saveAsTable("lookalike_trainready_jimmy")
