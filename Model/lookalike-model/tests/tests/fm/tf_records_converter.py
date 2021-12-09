from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, VectorIndexer, StringIndexer
from pyspark.sql.functions import broadcast
import argparse
import yaml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--configs", type=str, help='config_file')

    args, unknown = parser.parse_known_args()
    conf_path = args.configs

    with open(conf_path, 'r') as ymlfile:
        config = yaml.load(ymlfile)

    pipeline_save_path="lookLike_pipeline26oct"

    read_path = config["DATA_PREP"]["data_prep_write_path"]
    cols = config["DATA_PREP"]["selected_features"]
    pipeline_save_path = config["DATA_PREP"]["pickel_file_write_path"]
    tfrecords_path = config["DATA_PREP"]["tfrecords_write_path"]

    spark = SparkSession \
        .builder \
        .getOrCreate()

    df_log = spark.read.parquet(read_path)

    indexer_list=[]
    idx_columns=[]
    for col in cols:
        idx_col=col+"_id"
        idx_columns.append(idx_col)
        indexer_list.append(StringIndexer().setInputCol(col).setOutputCol(idx_col).setHandleInvalid("skip"))

    pipeline = Pipeline(stages=indexer_list)
    pipelineModel = pipeline.fit(df_log)
    df_log=pipelineModel.transform(df_log)

    pipelineModel.write().overwrite().save(pipeline_save_path)

    for col in idx_columns:
        df_log = df_log.withColumn(col, df_log[col].cast(IntegerType()))

    df_log = df_log.withColumn('features', udf(lambda *x: [i for i in x], ArrayType(IntegerType()))(*idx_columns))
    df_log.select(["features","is_click"]).write.format("tfrecords").option("recordType", "Example").mode('overwrite').save(tfrecords_path)

    print("job is completed")


