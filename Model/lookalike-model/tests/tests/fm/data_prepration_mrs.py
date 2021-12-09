from pyspark import SparkContext, SparkConf,HiveContext
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode
from pyspark.sql.functions import broadcast
import yaml
import argparse

def log_processor(hive_context, log_table, columns, flag):
    df_log = hive_context.sql(query.format(cols=columns, table_name=log_table))
    df_log = df_log.where(col(user_id_col).isNotNull())
    df_log = df_log.dropDuplicates()
    df_log = df_log.withColumn('is_click', lit(flag))
    return df_log

if __name__ == "__main__":
    print("job is started")
    parser = argparse.ArgumentParser()
    parser.add_argument("--configs", type=str, help='config_file')

    args, unknown = parser.parse_known_args()
    conf_path = args.configs

    with open(conf_path, 'r') as ymlfile:
        config = yaml.load(ymlfile)

    cols=config["DATA_PREP"]["selected_features"]
    clicklog_table_name= config["DATA_PREP"]["clicklog_table_name"]
    showlog_table_name= config["DATA_PREP"]["showlog_table_name"]
    persona_table_name= config["DATA_PREP"]["persona_table_name"]
    path=config["DATA_PREP"]["data_prep_write_path"]
    user_id_col=config["DATA_PREP"]["user_id_col"]
    huawei_ads_ctr=config["DATA_PREP"]["huawei_ads_ctr"]

    columns_str=",".join(cols)
    query = "select {cols} from {table_name}"

    sc = SparkContext()
    hive_context = HiveContext(sc)

    df_clicklog = log_processor(hive_context,clicklog_table_name,columns_str,1)
    df_showlog = log_processor(hive_context, showlog_table_name, columns_str, 0)

    bc_df_clicklog = broadcast(df_clicklog)
    df_showlog = df_showlog.join(bc_df_clicklog.select(user_id_col), on=[user_id_col], how="inner")
    df_showlog = df_showlog.join(bc_df_clicklog.select(cols), on=cols,how="leftanti")

    df_showlog=df_showlog.sample(huawei_ads_ctr, False)
    df_log = df_showlog.union(df_clicklog)

    df_log.write.mode("overwrite").parquet(path)

    print("job is completed")
