### What is predictor_dl_model?
predictor_dl_model is a suite of offline processes to forecast traffic inventory. The suite contains the following modules. More information is included in the module’s directory.

1.	datagen:  This module generates factdata table which contains traffic data. 
2.	trainer:  This module builds and trains a deep learning model based on the factdata table.
3.	pipeline: This module processes factdata table into training-ready data which is used to train the neural network.
    a. Main-ts only modifies the structure the raw data, does not remove any data.
    b. Pre-cluster denoises(new)/removes individual uckeys and prepare them for clustering
    c. Cluster creates clusters and denoises/removes clusters
    d. Distribution records the relationship between virtual-uckey and uckey
    e. Norm normalizes attributes
    f. Tfrecords, save data into tfrecords format
 

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2
Driver: Python 3.6, Spark Client 2.3, HDFS Client, tensorflow-gpu 1.10 

To install dependencies run:
pip install -r requirements.txt


### Install and Run
1.	Download the blue-martin/models project 
2.	Transfer the predictor_dl_model directory to ~/code/predictor_dl_model/ on a GPU machine which also has Spark Client.
3.  cd predictor_dl_model
4.  pip install -r requirements.txt to install required packages. These packages are install on top of python using pip.
5.  python setup install (to install predictor_dl_model package)
6.  (optional) python set_up.py bdist_egg (to create .egg file to provide to spark-submit)
7.	Follow the steps in ~/code/predictor_dl_model/datagen/README.md to generate data
8.	Go to directory ~/code/predictor_dl_model/predictor_dl_model
9.	Run run.sh or each script individually


### Documentation
Documentation is provided through comments in config.yml and README files

### Note
saved_model_cli show --dir <model_dir>/<version> --all
