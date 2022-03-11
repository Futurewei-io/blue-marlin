from pyspark import SparkContext
from pyspark.sql.functions import count, lit, col, udf, collect_list, explode, sqrt, mean
from pyspark.sql.types import IntegerType, StringType, MapType, ArrayType, BooleanType, FloatType
from pyspark.sql import SQLContext, HiveContext
import sys


def filter_slot_id(df, slot_ids=[]):
    if slot_ids and len(slot_ids) > 0:
        return df.filter(udf(lambda x: slot_ids.__contains__(x.split(",")[1]), BooleanType())(df.uckey))
    else:
        sys.exit("empty slot ids")


if __name__ == '__main__':

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    hive_context = HiveContext(sc)
    sc.setLogLevel('ERROR')

    dates = ['2021-06-02', '2021-06-03', '2021-06-04', '2021-06-05', '2021-06-06', '2021-06-07', '2021-06-08', '2021-06-09', '2021-06-10', '2021-06-11', '2021-06-12', '2021-06-13', '2021-06-14', '2021-06-15', '2021-06-16', '2021-06-17', '2021-06-18', '2021-06-19', '2021-06-20', '2021-06-21', '2021-06-22', '2021-06-23', '2021-06-24', '2021-06-25', '2021-06-26', '2021-06-27', '2021-06-28', '2021-06-29', '2021-06-30', '2021-07-01', '2021-07-02', '2021-07-03', '2021-07-04', '2021-07-05', '2021-07-06', '2021-07-07', '2021-07-08', '2021-07-09', '2021-07-10', '2021-07-11', '2021-07-12', '2021-07-13', '2021-07-14', '2021-07-15', '2021-07-16', '2021-07-17', '2021-07-18', '2021-07-19', '2021-07-20', '2021-07-21', '2021-07-22', '2021-07-23', '2021-07-24', '2021-07-25', '2021-07-26', '2021-07-27', '2021-07-28', '2021-07-29', '2021-07-30']
    table = 'factdata'

    for id in range(0, len(dates)-1):
        command = """select count_array,day,uckey from {} where day = '{}'"""
        print("Running command:", command.format(table, dates[id]))
        df = hive_context.sql(command.format(table, dates[id]))
        df = df.select(df.uckey, df.day, explode(df.count_array))
        df = df.withColumn('col', udf(lambda x: str(x).split(":"), ArrayType(StringType()))(df.col))
        df = df.select(df.uckey, df.day, df.col[1]).withColumnRenamed("col[1]", "actual_impr")
        df = df.withColumn('actual_impr', udf(lambda x: int(x), IntegerType())(df.actual_impr))
        df = df.groupBy('uckey').sum('actual_impr').withColumnRenamed("sum(actual_impr)", 'total')
        df.createOrReplaceTempView("impr_temp_table")

        command = """INSERT OVERWRITE TABLE {} PARTITION (pt_d='{}') select uckey, total from impr_temp_table""".format(
            'dws_pps_ims_impr_his_data_dm', dates[id])

        hive_context.sql(command)
        print('Processed data for ', dates[id])

    sc.stop()