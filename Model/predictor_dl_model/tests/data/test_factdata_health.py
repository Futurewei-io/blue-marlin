"""
1. It gets a table name and time [optional, default is 600 sec] as an argument.
2. Check if the table has partitions or not.
3. If there is a partition available it goes to check count and distinct count and bucket mapping health.
4. If any of these functions running time exceed timeout, the spark job would be terminated.

"""

from pyspark.sql import HiveContext
from pyspark import SparkContext
import time
import argparse
import multiprocessing

parser = argparse.ArgumentParser(description='Short sample app')
parser.add_argument("--table_name")
parser.add_argument("--timeout", default=600)
args = parser.parse_args()
table_name = str(args.table_name)
timer = int(args.timeout)


def load_df(hive_context, table_name, bucket_id):
    command = """select * from {} where bucket_id = {}""".format(table_name, bucket_id)
    return hive_context.sql(command)


def distinct_count(df, column, return_dic):
    return_dic["distinct_count"] = df.select(column).distinct().count()


def total_count(df, return_dic):
    return_dic["total_count"] = df.count()


def bucket_id_health(df1, df2, return_dic):
    join_df = df1.join(df2, ['uckey'], 'inner')
    if join_df.count() > 0:
        return_dic["bucket_id_health"] = ["buckets have over lap..."]
    else:
        return_dic["bucket_id_health"] = ["buckets look good..."]


def total_check(df, return_dic, timeout):
    p = multiprocessing.Process(target=total_count, name="total", args=(df, return_dic))
    p.start()
    time.sleep(timeout)

    if p.is_alive():
        print(p.name, "is still running... the time is out, let's kill it...")
        p.terminate()
    else:
        print("total number of record is: ", return_dic["total_count"])


def total_distinct_check(df, column, return_dic, timeout):
    p = multiprocessing.Process(target=distinct_count, name=column, args=(df, column, return_dic))
    p.start()
    time.sleep(timeout)

    if p.is_alive():
        print(p.name, "is still running... the time is out, let's kill it...")
        p.terminate()
    else:
        print("Total distinct number of", column, "is: ", return_dic["distinct_count"])


def bucket_check(df, df2, return_dic, timeout):
    p = multiprocessing.Process(target=bucket_id_health, name="bucket id check", args=(df, df2, return_dic))
    p.start()
    time.sleep(timeout)

    if p.is_alive():
        print(p.name, "is still running... the time is out, let's kill it...")
        p.terminate()
    else:
        print(return_dic["bucket_id_health"])


# def bucket_id_health (df, return_dic):
#     temp_df = df.groupBy('uckey').agg(countDistinct('bucket_id').alias("count_bucket"))
#     if temp_df.filter(f.col('count_bucket') > 1).count() > 0:
#          return_dic[bucket_id_health] = ["uckeys are assigned to multiple bucket_id"]
#     else:
#         return_dic[bucket_id_health] = ["bucket assignment is good"]

def run():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    hive_context = HiveContext(sc)
    df = load_df(hive_context=hive_context, table_name=table_name, bucket_id=1)
    df2 = load_df(hive_context=hive_context, table_name=table_name, bucket_id=2)

    manager = multiprocessing.Manager()
    return_dic = manager.dict()

    try:
        command = """show PARTITIONS {}""".format(table_name)
        hive_context.sql(command)
        print("The partitions look good!!")
        total_check(df, return_dic, timer)
        total_distinct_check(df, 'day', return_dic, timer)
        total_distinct_check(df, 'uckey', return_dic, timer)
        bucket_check(df, df2, return_dic, timer)

    except Exception:
        print("There is no partitions available at this table ... No more health check would perform at this time!! ")


if __name__ == '__main__':
    run()
