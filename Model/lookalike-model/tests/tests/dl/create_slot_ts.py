from pyspark import SparkContext
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, FloatType, BooleanType
import statistics
import math

yesterday="2020-06-10"
past_days=101
data_read_path="slotid_100ts_data"



def calculate_time_series(df, day_list):
    def _helper(ts_list_map):
        ts_map = {}
        result = []
        for item_map in ts_list_map:
            for day, value in item_map.items():
                ts_map[day] = value
        for day in day_list:
            if day in ts_map:
                count = int(ts_map[day])
            else:
                count = 0
            result.append(count)
        return result
    _udf = udf(_helper, ArrayType(IntegerType()))
    df = df.withColumn('ts', _udf(df.ts_list_map))
    return df

def ts_datapoint_100(x_list):
    count=0
    for x in x_list:
        if x>0:
            count=count+1
    if count==100:
        return True
    return False


    # def normalize(mlist):
    #     avg = statistics.mean(mlist)
    #     std = statistics.stdev(mlist)
    #     return avg, std


day = datetime.strptime(yesterday, '%Y-%m-%d')
day_list = []
for _ in range(0, past_days):
    day_list.append(datetime.strftime(day, '%Y-%m-%d'))
    day = day + timedelta(days=-1)

day_list.remove("2020-06-01")
day_list.sort()
sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.parquet(data_read_path)

df = df.groupBy('slot', 'day').agg(sum('impr_count').alias('impr_count'))

df = df.withColumn('day_count', expr("map(day, impr_count)"))

df = df.groupBy("slot").agg(collect_list('day_count').alias('ts_list_map'))

# This method handles missing dates by injecting nan

df = calculate_time_series(df, day_list)

df = df.filter(udf(lambda x_list: ts_datapoint_100(x_list), BooleanType())(df.ts))
df = df.withColumn('x', udf(lambda ts: ts[:-40], ArrayType(IntegerType()))(df.ts))
df = df.withColumn('y', udf(lambda ts: ts[-40:], ArrayType(IntegerType()))(df.ts))

def data_smoothing(ts,period=7):
    ll=[0.0]*len(ts)
    for i in range(len(ts)):
        if i < period:
            ll[i] = float(ts[i])
        else:
            ll[i] = float(statistics.mean(ts[i - period:i + 1]))
    return ll

udf_data_smoothing = udf(data_smoothing, ArrayType(FloatType()))
df = df.withColumn('ts', udf_data_smoothing(df.ts))

df = df.withColumn('ts', udf(lambda ts: [math.log(i) for i in ts ], ArrayType(FloatType()))(df.ts))
# df = df.withColumn('mean', udf(lambda ts: statistics.mean(ts), FloatType())(df.ts))
# df = df.withColumn('std', udf(lambda ts: statistics.stdev(ts), FloatType())(df.ts))
# df = df.withColumn('stats', udf(lambda ts: [statistics.mean(ts),statistics.stdev(ts)], ArrayType(FloatType()))(df.ts))
# df = df.select('ts', 'y', explode(df.stats))
# df = df.select('ts', 'y', df.col[0],df.col[1]).withColumnRenamed("col[0]", 'mean').withColumnRenamed("col[1]", 'std')
# df = df.withColumn('x', udf(lambda ts,mean,std: [(i-mean)/std for i in ts[:-40] ], ArrayType(FloatType()))(df.ts,df.mean,df.std))

df = df.select("slot",'x', 'y',"ts","mean","std")

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

slot_ud = StringIndexer().setInputCol("slot").setOutputCol("slot_id").setHandleInvalid("skip")
pipeline = Pipeline(stages=[slot_ud])
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)
# df = df.select("slot_id",'x', 'y',"ts","mean","std")
# df = df.withColumn('slot_id', udf(lambda slot_id: int(slot_id), IntegerType())(df.slot_id))

# df.coalesce(100).write.mode("overwrite").parquet("seq2seq_ts_raw_31")
# df.select("slot_id",'x', 'y',"ts","mean","std").write.format("tfrecords").option("recordType", "Example").mode('overwrite').save("tf_data_16dec")
df.select("slot_id",'x', 'y',"ts").write.format("tfrecords").option("recordType", "Example").mode('overwrite').save("tf_ts_raw_data_21dec")

