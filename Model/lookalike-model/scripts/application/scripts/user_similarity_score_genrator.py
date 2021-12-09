from pyspark.ml.feature import VectorAssembler, BucketedRandomProjectionLSH, Normalizer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql import HiveContext
from pyspark import SparkContext, SQLContext
import sys
import yaml
import argparse

'''
properties_path: Mandatory argument 
seed_user_data_path : Optional argument
1) if similarity calculation is done on complete data
    cmd:   spark-summit user_similarity_score_genrator.py properties_path
'''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument("config_file", help='please provide config file')
    args = parser.parse_args()

    with open(args.config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    keyword_score_table = cfg["keyword_score_table"]
    bucketLength = cfg["bucketLength"]
    numHashTables = cfg["numHashTables"]
    minimum_similarity_score = cfg["minimum_similarity_score"]
    write_path = cfg["write_path"]

    if minimum_similarity_score <= 0 or minimum_similarity_score >= 1:
        sys.exit("minimum similarity score must be between 0 to 1 minimum_similarity_score={}".format(
            minimum_similarity_score))

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    hive_context = HiveContext(sc)

    query = "select * from {}".format(keyword_score_table)
    print("hive query: ", query)
    df = hive_context.sql(query).select("aid", "kws")

    df = df.withColumn('scores', udf(lambda x: x.values(), ArrayType(FloatType))(df.kws))

    keywords_length = len(df.select("scores").take(1)[0]["scores"])
    assembler = VectorAssembler(inputCols=["scores[{}]".format(i) for i in range(keywords_length)],
                                outputCol="user_score")
    ddf = assembler.transform(df.select("*", *(df["scores"].getItem(i) for i in range(keywords_length)))).select(
        "user_score")

    normalizer = Normalizer(inputCol="user_score", outputCol="normFeatures", p=2.0)
    extended_user_df = normalizer.transform(ddf)
    extended_user_df.cache()

    seed_user_df = extended_user_df

    # LSH Algorithm
    brp = BucketedRandomProjectionLSH(inputCol="normFeatures", outputCol="hashes", bucketLength=bucketLength,
                                      numHashTables=numHashTables)
    lsh = brp.fit(extended_user_df)
    df_users = lsh.approxSimilarityJoin(seed_user_df, extended_user_df, 1 - minimum_similarity_score,
                                        distCol="EuclideanDistance")
    df_users = df_users.withColumn('similarity_score', udf(lambda x: 1 - x, FloatType)(df_users.EuclideanDistance))

    df_users.coalesce(1000).write.mode('overwrite').parquet(write_path)

    sc.stop()

    print("job is completed")
