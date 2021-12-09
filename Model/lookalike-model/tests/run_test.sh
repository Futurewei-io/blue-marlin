#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# test_main_logs: identifies the keywords that have traffic greater than a set percentage.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_keywords.py
fi

# test_main_clean: preparing cleaned persona, click and show logs data.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_clean.py
fi

# test_main_logs: merges click and show log data.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_logs.py
fi

# test_main_trainready: aggregates the click and show log data by user.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict pipeline/test_main_trainready.py
fi

# test_score_matrix_table: Converts the score vector table into matrices.
if false
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict application/pipeline/test_score_matrix_table.py
fi

# test_top_n_similarity_table_generator: Finds the top n users similar to the given user.
if false
then
    # export PYSPARK_DRIVER_PYTHON=python
    # export PYSPARK_PYTHON=./environment/bin/python

    spark-submit --master yarn --num-executors 5 --executor-cores 2 \
    --conf spark.hadoop.hive.exec.dynamic.partition=true \
    --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
    --conf "spark.yarn.dist.archives=lookalike-application-python-venv.tar.gz#environment" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/lookalike-application-python-venv/bin/python" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=./environment/lookalike-application-python-venv/bin/python" \
    application/pipeline/test_top_n_similarity_table_generator_1.py

    spark-submit --master yarn --num-executors 5 --executor-cores 2 \
    --conf spark.hadoop.hive.exec.dynamic.partition=true \
    --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
    --conf "spark.yarn.dist.archives=lookalike-application-python-venv.tar.gz#environment" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/lookalike-application-python-venv/bin/python" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=./environment/lookalike-application-python-venv/bin/python" \
    application/pipeline/test_top_n_similarity_table_generator_2.py
fi

# pipeline_v2/test_balance_clusters.py
if true
then
    spark-submit --master yarn --num-executors 5 --executor-cores 2 --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict application/pipeline_v2/test_balance_clusters.py
fi

