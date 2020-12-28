### What is ctr score generator?
CTR score generator is an offline process to generate keyword scores for gucdocs.

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 3.6, Spark Client 2.3

### Install and Run
1.	Download the bluemarlin-services/performance-forecasting/ctr_score_generator project.
2.	Transfer the bluemarlin-services/performance-forecasting/ctr_score_generator to ~/bluemarlin-services/.
    performance-forecasting/ctr_score_generator on a machine which also has Spark Client.
3.  cd performance-forecasting.
4.  cd ctr_score_generator.
5.  python setup install (to install ctr_score_generator package).
6.  pip install -r requirements.txt
7.	Run run.sh or follow up the steps in run.sh.
