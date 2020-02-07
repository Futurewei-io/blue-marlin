from dlpredictor.prediction.ims_predictor_util import convert_records_map_to_list
from dlpredictor.log import *
from dlpredictor.util.sparkesutil import *

import json


def predict_counts_for_uckey(serving_url, forecaster, model_stats, cfg):
    def __predict_counts_for_uckey(uckey, day_hour_counts):
        ucdoc = convert_day_hour_counts_to_ucdoc(uckey, day_hour_counts)
        records = convert_records_map_to_list(ucdoc.records)
        # records is a list of ucdays
        ucdoc.predictions = forecaster.dl_forecast(serving_url=serving_url, model_stats=model_stats, ucdoc=ucdoc, ucday_list=records)
        ucdoc.records = None
        ucdoc.bookings = []
        return json.dumps(ucdoc, default=lambda x: x.__dict__)
    return __predict_counts_for_uckey


def format_data(x, field_name):
    _doc = {'uckey': x[0], field_name: json.loads(x[1])}
    return (x[0], json.dumps(_doc))
