we run under conda
source activate py27

ln -s /home/reza/git/PPS-IMS/FIBased/common/ims_common ims_common
ln -s /home/reza/git/PPS-IMS/FIBased/predictor/src/predictor predictor
ln -s /home/reza/git/PPS-IMS/FIBased/tbr/src/tbr tbr

this is for change ownership for log files
sudo chown -R reza huawei/

make sure have the following
ll /home/reza/eshadoop/elasticsearch-hadoop-6.5.2/dist/

If you run spark-submit for spark-es from console do not forget
--jars /home/reza/eshadoop/elasticsearch-hadoop-6.5.2/dist/elasticsearch-hadoop-6.5.2.jar

