### What is optimizer?
Optimizer is a process to optimize the allocation of bookings over inventory. 
Optimizer also creates an allocation map for the future days.

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 3.6, Spark Client 2.3

To install dependencies run:
pip install -r requirements.txt


### Install and Run
1.	Download the blue-martin/optomizer project
2.	Transfer the optomizer directory to ~/code/optimizer on a machine which also has Spark Client.
3.  cd optomizer
4.  pip install -r requirements.txt (to install required packages)
5.  python setup.py install (to install optimizer package)
6.	Run run.sh 

### Documentation
Documentation is provided through comments in config.yml and README files.

1. Optimizer runs by providing a <DAY> as argument.
2. It reads all the bookings that happen after <DAY> and regenrates booking-buckets.
3. It will lock on booking, soft-delete all existing booking-buckets and write the new ones.
4. The booking-buckets for <TOMORROW> are used for allocation-plan and gets saved in HDFS.
5. The bookings that are added/deleted during optimizer process are not participating in optimization process.
6. The bookings that are added during optimizer process will be applied using imsservice booking service. If adding process receive error then the optimization process has been failed and needs to be reversed.
7. The bookings that are deleted during optimizer process will be considered in the next optimizer run.