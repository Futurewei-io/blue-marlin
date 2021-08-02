from pyspark import SparkContext, SparkConf,SQLContext
from pyspark.sql.functions import count, lit, col, udf, expr, collect_list, explode
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, BooleanType,FloatType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from pyspark.sql.functions import broadcast

def _list_to_map(count_array):
    count_map = {}
    for item in count_array:
        key_value = item.split(':')
        count_map[key_value[0]] = key_value[1]
    return count_map


def add_count_map(df):
    # Convert count_array to count_map
    list_to_map_udf = udf(_list_to_map, MapType(
        StringType(), StringType(), False))
    df = df.withColumn('count_map', list_to_map_udf(df.count_array))
    return df

def variance(plist):
    l=len(plist)
    ex=sum(plist)/l
    ex2=sum([i*i for i in plist])/l
    return ex2-ex*ex



query="select count_array,day,uckey from factdata where day in ('2020-05-15','2020-05-14','2020-05-13','2020-05-12','2020-05-11','2020-05-10','2020-05-09')"
sc = SparkContext()
hive_context = HiveContext(sc)

df = hive_context.sql(query)
df = add_count_map(df)

df = df.select('uckey', 'day', explode(df.count_map)).withColumnRenamed("value", "impr_count")

df = df.withColumn('impr_count', udf(lambda x: int(x), IntegerType())(df.impr_count))
df = df.groupBy('uckey', 'day').sum('impr_count').withColumnRenamed("sum(impr_count)", 'impr_count')


split_uckey_udf = udf(lambda x: x.split(","), ArrayType(StringType()))
df = df.withColumn('col', split_uckey_udf(df.uckey))
df = df.select('uckey','impr_count', 'day', df.col[1]).withColumnRenamed("col[1]", 'slot_id')


df_slot=df.select('slot_id','impr_count', 'day')
df_slot=df_slot.groupBy('slot_id','day').sum('impr_count').withColumnRenamed("sum(impr_count)", "impr_total")
bc_df_slot = broadcast(df_slot)

df_new = df.join(bc_df_slot, on=["slot_id",'day'],how="inner")

df_new = df_new.withColumn('percent', udf(lambda x,y: (x*100)/y, FloatType())(df_new.impr_count,df_new.impr_total))


df2=df_new.groupBy("uckey").agg(collect_list('percent').alias('percent'))
df2 = df2.withColumn('var', udf(lambda x: variance(x), FloatType())(df2.percent))
df2.select("uckey","var").orderBy(["var"],ascending=False).show(300,truncate=False)
df2.cache()
print("% uckeys having varience > 0.01  ",df2.filter((df2.var <= 0.01)).count()*100/df2.count())

