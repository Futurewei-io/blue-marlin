### Pipleline Steps
Pipeline takes the following steps:

1. Reads factdata from hive table from day(-365)(configurable) to day(-1), input is day(-1). day(0) is today and day(-1) is yesterday.
2. Processes data using spark and writes results into tfrecords e.g. factdata.tfrecords.<date> (configurable)
3. Starts trainer to read the rfrecords and create the model
4. Writes model into local directory
5. Compare the new model and old model (new model evaluation)(future)
6. Set the predictor to use the new model - predictor reads the name of the model that it uses from Ealsticsearch (future)

### UCKEY Elements for IMPRESSION table
uckey consists of the following items.

ucdoc.m = parts[0] #media-type
ucdoc.si = parts[1] #slot-id
ucdoc.t = parts[2] #connection-type
ucdoc.g = parts[3] #gender
ucdoc.a = parts[4] #age
ucdoc.pm = parts[5] #price-model
ucdoc.r = parts[6] #resident-location
ucdoc.ipl = parts[7] #ip-location


### UCKEY Elements for REQUEST table
uckey consists of the following items.

ucdoc.m = parts[0] #media-type
ucdoc.si = parts[1] #slot-id
ucdoc.t = parts[2] #connection-type
ucdoc.g = parts[3] #gender
ucdoc.a = parts[4] #age
ucdoc.r = parts[5] #resident-location
ucdoc.ipl = parts[6] #ip-location
