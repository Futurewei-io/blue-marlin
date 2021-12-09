import pickle
import yaml
import argparse
from pyspark.sql import SparkSession

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--configs", type=str, help='config_file')

    args, unknown = parser.parse_known_args()
    conf_path = args.configs

    with open(conf_path, 'r') as ymlfile:
        config = yaml.load(ymlfile)

    read_path = config["DATA_PREP"]["data_prep_write_path"]
    selected_features = config["DATA_PREP"]["selected_features"]
    write_path = config["DATA_PREP"]["pickel_file_write_path"]

    spark = SparkSession \
        .builder \
        .getOrCreate()

    df_log = spark.read.parquet(read_path)

    count_dict = {}
    for col in selected_features:
        config["MODEL"]["DEEP_FM_FEATURES"][col]["unique_count"] = df_log.select(col).distinct().count() + 1

    with open(write_path, 'wb') as f:
        pickle.dump(config, f, pickle.HIGHEST_PROTOCOL)

    print(config["MODEL"])
    print("job is completed")