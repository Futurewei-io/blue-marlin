### How to Run
main.py is run via pipeline run.sh

To run individually:

cd .../predictor_dl_model
spark-submit --jars spark-tensorflow-connector_2.11-1.15.0.jar pipeline/main.py config.yml [1]

The last parameter defines the steps to be run by main.py. It can have the following values:
[1]: Runs data prepartion and saves train-ready results into hive table
[2]: Reads train-ready data from hive table and saves it as tfrecords
[1,2]: Does 1 and 2

### Pipleline Steps
Pipeline takes the following steps:

1. Reads factdata from hive table from day(-365)(configurable) to day(-1), input is day(-1). day(0) is today and day(-1) is yesterday.
2. Processes data using spark and writes results into tfrecords e.g. factdata.tfrecords.<date> (configurable)
3. Starts trainer to read the rfrecords and create the model
4. Writes model into local directory
5. Compare the new model and old model (new model evaluation)(future)
6. Set the predictor to use the new model - predictor reads the name of the model that it uses from Ealsticsearch (future)
