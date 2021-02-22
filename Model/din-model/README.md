### What is din_model?
The DIN model targets to setup the correlation between UCDocs and ads based on logged user behavior.  The log-data is usually noisy due to various reasons, such as: unstable slot_id, redundant records, missing data, etc. The data pre-processing is then required to clean original log-data before the data can be feed into the DIN model. Cleaning steps include transforming the log-data to new data format, and generating new data tables.

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 2.7, Spark Client 2.3, HDFS Client, tensorflow-gpu 1.10 

To install dependencies run:
pip install -r requirements.txt

### Install and Run
    1. Download the blue-martin/models/din-model project
    2. Transfer the din-model directory to ~/code/din-model/ on a GPU machine (Optianl) which also has Spark Client.
    3. Refer to INPUT.md for input data schema
    4. cd din-model
    5. pip install -r requirements.txt (to install required packages)
    6. Go to directory ~/code/din_model/din_model
    7. Run run.sh (Make sure flags are set to true)(If the GPU machine is not as the same as Spark Client, then move tfrecords to the trainer machine)

### Run Using Airflow
Refer to AIRFLOW.md

### Documentation
More documentation is provided through comments in config.yml and README files

