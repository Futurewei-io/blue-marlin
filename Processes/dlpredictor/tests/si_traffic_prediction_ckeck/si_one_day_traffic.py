import pyspark.sql.functions as fn
from pyspark.sql.types import IntegerType


def get_traffic(day_index, si):
    df = sql('select * from dlpm_111021_no_residency_no_mapping_tmp_ts')
    df = df.filter("si='{}'".format(si))
    df = df.withColumn('oneday', fn.udf(lambda x: x[day_index], IntegerType())(df.ts_ver))
    df.agg(fn.sum('oneday')).show(1, False)


def get_traffic_1(day_index, si):
    df = sql('select * from dlpm_110421_no_residency_no_mapping_tmp_ts')
    df = df.filter("si='{}'".format(si))
    df = df.withColumn('oneday', fn.udf(lambda x: x[day_index], IntegerType())(df.ts))
    df.agg(fn.sum('oneday')).show(1, False)

def get_traffic_2(day_index, si):
    df = sql('select * from dlpm_110421_no_residency_no_mapping_tmp_pre_cluster')
    df = df.filter("si='{}'".format(si))
    df = df.withColumn('oneday', fn.udf(lambda x: x[day_index], IntegerType())(df.ts))
    df.agg(fn.sum('oneday')).show(1, False)





'''

POST dlpredictor_110421_no_residency_no_mapping_predictions/_search
{
  "size": 0, 
  "query": {
    "bool": {
      "must": [
        {"match": {
          "ucdoc.si": "z041bf6g4s"
        }}
      ]
    }
  },
   "aggs": {
        "total": {
            "sum": {
                "script": """
                  def v =  doc['ucdoc.predictions.2021-07-28.hours.total'];
                  return v
                """
            }
        }
    }
}

'''