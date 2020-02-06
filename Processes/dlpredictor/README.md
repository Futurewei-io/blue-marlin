### What is dlpredictor?
dlpredictor is an offline processe to forecast traffic inventory.

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 3.6, Spark Client 2.3

To install dependencies run:
pip install -r requirements.txt


### Install and Run
1.	Download the blue-martin/dlpredictor project
2.	Transfer the dlpredictor directory to ~/code/dlpredictor on a machine which also has Spark Client.
3.  cd dlpredictor
4.  pip install -r requirements.txt (to install required packages)
5.  python setup install (to install predictor_dl_model package)
6.	Run run.sh 

### Documentation
Documentation is provided through comments in config.yml and README files