#  Licensed to the Apache Software Foundation (ASF) under one

import pyspark.sql.functions as fn
from pyspark import SparkContext
from pyspark.sql import HiveContext

''' Get the ts table and canculate the traffic in each day for each SI '''

def run(hive_context, input_table_name):
    
    # Read factdata table
    command = """
    SELECT si,ts FROM {}
    """.format(input_table_name)

    # DataFrame[ts: array<int>]
    df = hive_context.sql(command)
    columns = ['ts']
    df_sizes = df.select(*[fn.size(col).alias(col) for col in columns])
    df_max = df_sizes.agg(*[fn.max(col).alias(col) for col in columns])
    max_dict = df_max.collect()[0].asDict()
    df_result = df.select('si', *[df[col][i] for col in columns for i in range(max_dict[col])])
    df_result = df_result.na.fill(value=0)
    df_result.toPandas().to_csv('total_si')



if __name__ == '__main__':
    sc = SparkContext()
    hive_context = HiveContext(sc)

    run(hive_context=hive_context, input_table_name="dlpm_10052021_1400_tmp_ts")