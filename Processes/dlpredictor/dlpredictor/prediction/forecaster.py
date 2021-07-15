#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from predictor_dl_model.trainer.client_rest_dl2 import predict
from dlpredictor import transform

def get_model_stats(hive_context, model_stat_table):
    '''
    return a dict
    model_stats = {
        "model": {
            "name": "s32",
            "version": 1,
            "duration": 90,
            "train_window": 60,
            "predict_window": 10
        },
        "stats": {
            "g_g_m": [
                0.32095959595959594,
                0.4668649491714752
            ],
            "g_g_f": [
                0.3654040404040404,
                0.4815635452904544
            ],
            "g_g_x": [
                0.31363636363636366,
                0.46398999646418304
            ],
    '''
    command = """
            SELECT * FROM {}
            """.format(model_stat_table)
    df = hive_context.sql(command)
    rows = df.collect()
    if len(rows) != 1:
        raise Exception('Bad model stat table {} '.format(model_stat_table))
    model_info = rows[0]['model_info']
    model_stats = rows[0]['stats']
    result = {
        'model': model_info,
        'stats': model_stats
    }
    return result


class Forecaster:

    def __init__(self, cfg):
        self.holiday_list = cfg['holiday_list']
        self.cfg = cfg

    def dl_daily_forecast(self, serving_url, model_stats, day_list, ucdoc_attribute_map):
        x, y = predict(serving_url=serving_url, model_stats=model_stats, day_list=day_list, ucdoc_attribute_map=ucdoc_attribute_map, forward_offset=0)
        ts = x[0]
        days = y
        return ts, days

# spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 32G --driver-memory 32G --py-files dist/dlpredictor-1.6.0-py2.7.egg,lib/imscommon-2.0.0-py2.7.egg,lib/predictor_dl_model-1.6.0-py2.7.egg --conf spark.driver.maxResultSize=5G 
if __name__ == '__main__':

    from pyspark import SparkContext
    from pyspark.sql import HiveContext
    from pyspark.sql.functions import udf, expr, collect_list, struct
    from pyspark.sql.types import StringType, ArrayType, MapType, FloatType, StructField, StructType

    cfg = {'holiday_list': ['2019-11-09', '2019-11-10', '2019-11-11', '2019-11-25', '2019-11-26', '2019-11-27','2019-11-28', '2019-12-24','2019-12-25', '2019-12-26','2019-12-31', '2020-01-01', '2020-01-02', '2020-01-19','2020-01-20', '2020-01-21', '2020-01-22', '2020-01-23',  '2020-01-24',  '2020-01-25', '2020-02-08']}

    forecaster = Forecaster(cfg)
    sc = SparkContext()
    hive_context = HiveContext(sc)

    day_list = [u'2020-03-01', u'2020-03-02', u'2020-03-03', u'2020-03-04', u'2020-03-05', u'2020-03-06', u'2020-03-07', u'2020-03-08', u'2020-03-09', u'2020-03-10', u'2020-03-11', u'2020-03-12', u'2020-03-13', u'2020-03-14', u'2020-03-15', u'2020-03-16', u'2020-03-17', u'2020-03-18', u'2020-03-19', u'2020-03-20', u'2020-03-21', u'2020-03-22', u'2020-03-23', u'2020-03-24', u'2020-03-25', u'2020-03-26', u'2020-03-27', u'2020-03-28', u'2020-03-29', u'2020-03-30', u'2020-03-31', u'2020-04-01', u'2020-04-02', u'2020-04-03', u'2020-04-04', u'2020-04-05', u'2020-04-06', u'2020-04-07', u'2020-04-08', u'2020-04-09', u'2020-04-10', u'2020-04-11', u'2020-04-12', u'2020-04-13', u'2020-04-14', u'2020-04-15', u'2020-04-16', u'2020-04-17', u'2020-04-18', u'2020-04-19', u'2020-04-20', u'2020-04-21', u'2020-04-22', u'2020-04-23', u'2020-04-24', u'2020-04-25', u'2020-04-26', u'2020-04-27', u'2020-04-28', u'2020-04-29', u'2020-04-30', u'2020-05-01', u'2020-05-02', u'2020-05-03', u'2020-05-04', u'2020-05-05', u'2020-05-06', u'2020-05-07', u'2020-05-08', u'2020-05-09', u'2020-05-10', u'2020-05-11', u'2020-05-12', u'2020-05-13', u'2020-05-14', u'2020-05-15', u'2020-05-16', u'2020-05-17', u'2020-05-18', u'2020-05-19', u'2020-05-20', u'2020-05-21', u'2020-05-22', u'2020-05-23', u'2020-05-24', u'2020-05-25', u'2020-05-26', u'2020-05-27', u'2020-05-28', u'2020-05-29', u'2020-05-30', u'2020-05-31', u'2020-06-01', u'2020-06-02', u'2020-06-03', u'2020-06-04', u'2020-06-05', u'2020-06-06', u'2020-06-07', u'2020-06-08', u'2020-06-09', u'2020-06-10', u'2020-06-11', u'2020-06-12', u'2020-06-13', u'2020-06-14', u'2020-06-15', u'2020-06-16', u'2020-06-17', u'2020-06-18', u'2020-06-19', u'2020-06-20']
    serving_url = 'http://10.193.217.105:8502/v1/models/dl_20210609:predict'
    model_stats = get_model_stats(hive_context, 'dlpm_06242021_1635_model_stat')
    print(model_stats)
    
    df = hive_context.sql('select * from dlpm_06242021_1500_trainready').where('uckey="native,d9jucwkpr3,4G,g_m,2,CPC,61,8" and price_cat=1')
    print(df.take(1)[0])

    result = transform.predict_daily_uckey(days=day_list,serving_url=serving_url, forecaster=forecaster, model_stats=model_stats, columns=df.columns)
    print(result)



