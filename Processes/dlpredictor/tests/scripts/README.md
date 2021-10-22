### What is dlpredictor tests?
dlpredictor tests is an offline testing module to test dl predictor API.

### Prerequisites
The same to the dl predictor as follows.
Elasticsearch and Pyspark are required.
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 3.6, Spark Client 2.3

To install dependencies run:
pip install -r requirements.txt

### Run the tests
1. Go to the directory of '/dlpredictor'.
2. Run the following spark-submit shell script:
spark-submit --jars lib/elasticsearch-hadoop-6.5.2.jar tests/test_dlpredictor_api.py tests/conf/config.yml model_name model_version DL_PREDICTOR_API_URL
Take the run_test_dlpredictor_api.sh as a reference.
3. Run the test_*.py files. e.g. 'python tests/test_predict_counts_for_uckey.py'.
4. Pay attention to the conf/config.yml.

### Documentation
Documentation is provided through comments in config.yml and README files.
