from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum as _sum, split
from pyspark.sql.types import IntegerType, DoubleType, StringType, MapType, ArrayType, FloatType, BooleanType, DataType
from pyspark.sql import SQLContext, HiveContext
from datetime import datetime, timedelta

def filter_slot_id(df,slot_ids=[]):
    if slot_ids and len(slot_ids)>0:
        return df.filter(udf(lambda x: slot_ids.__contains__(x.split(",")[1]), BooleanType())(df.uckey))
    else:
        sys.exit("empty slot ids")

slot_ids=['17dd6d8098bf11e5bdec00163e291137', '5cd1c663263511e6af7500163e291137', '7b0d7b55ab0c11e68b7900163e3e481d', 'a168ku91xw', 'a290af82884e11e5bdec00163e291137', 'a5qx69o4vz', 'a7wm1jddy5', 'b3rcgwixmh', 'b9fwp0ybqg', 'd4d7362e879511e5bdec00163e291137', 'd971z9825e', 'e351de37263311e6af7500163e291137', 'f4dos9zs9w', 'f6105fe269aa11e6af7500163e291137', 'k4werqx13k', 'm7phcmp1xr', 'o9f80sd41l', 's78ihtn26r', 't0rfukuqzt', 'u4mjye1nb9', 'w3h460lg2v', 'w9fmyd5r0i', 'x2fpfbm8rt', 'z0rztju3pf', 'z7f1g6z66k', 'z8d74936mf']
if __name__ == "__main__":
    
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sqlcontext = SQLContext(sc)
    sc.setLogLevel("ERROR")

    df = hive_context.sql("""select * from ADS_PPS_IMS_LOG_PROCESSOR_FACTDATA_DM where day in ('2021-07-21', '2021-07-22', '2021-07-23', '2021-07-24', '2021-07-25', '2021-07-26', '2021-07-27', '2021-07-28', '2021-07-29', '2021-07-30', '2021-07-31')""")
    df = filter_slot_id(df,slot_ids)
    df = df.select(df.uckey, df.day, explode(df.count_array))
    df = df.withColumn('col', udf(lambda x: str(x).split(":"), ArrayType(StringType()))(df.col))
    df = df.select(df.uckey, df.day,df.col[1]).withColumnRenamed("col[1]","impr_count")
    df = df.withColumn('impr_count', udf(lambda x: int(x), IntegerType())(df.impr_count))
    df = df.groupBy('uckey','day').agg(_sum('impr_count').alias('actual_impr'))
    df = df.withColumn('uckey_split', split('uckey', ','))
    print("=================Factdata impressions================")
    df.groupby(df.day,df.uckey_split[1]).sum('actual_impr').orderBy('day').show(5000,False)

    df = sqlcontext.read.parquet("prediction/dl")
    df = df.withColumn('uckey_split', split('uckey', ','))
    print("==============Predicted Impressions================")
    df.groupby(df.day,df.uckey_split[1]).sum('impr').orderBy('day').show(500,False)
