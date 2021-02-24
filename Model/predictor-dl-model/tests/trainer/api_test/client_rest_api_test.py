from predictor_dl_model.trainer.client_rest import predict
import numpy as np
from datetime import datetime
from datetime import timedelta

def calculate_MAPE_ERROR(testing_values:list, predicted_values:list) -> float:
    MAPE_ERROR = np.average(np.divide(np.abs(np.subtract(testing_values, predicted_values)), predicted_values))
    return MAPE_ERROR

def test_dlpredictor_api():
    TENSORFLOW_SERVICEAPI_URL = "http://10.193.217.105:8501/v1/models/bob:predict"

    model_stats = {
        "model": {
            "name": "s32",
            "version": 1,
            "duration": 90,
            "train_window": 60,
            "predict_window": 10, # should be <= FORWARD_OFFSET in seq2seq model
            "FORWARD_OFFSET": 11
        },
        "stats": {# Mean and variance ?
            "g_g_m": [0.32095959595959594, 0.4668649491714752],
            "g_g_f": [0.3654040404040404, 0.4815635452904544],
            "g_g_x": [0.31363636363636366, 0.46398999646418304],
            "a_1": [0.198989898989899, 0.3992572317838901],
            "a_2": [0.2474747474747475, 0.4315630593164027],
            "a_3": [0.295959595959596, 0.45649211860504146],
            "a_4": [0.25757575757575757, 0.43731748751040456],
            "t_3G": [0.3565656565656566, 0.4790051176675845],
            "t_4G": [0.29772727272727273, 0.45727819223458205],
            "t_5G": [0.3457070707070707, 0.4756182644159981],
            "si_1": [0.37424242424242427, 0.4839470491115894],
            "si_2": [0.4042929292929293, 0.49077533664980666],
            "si_3": [0.22146464646464648, 0.4152500106648333],
            "price_cat_0": [0.0, 1.0],
            "price_cat_1": [0.3333333333333333, 0.4714243623012701],
            "price_cat_2": [0.3333333333333333, 0.47142436230126994],
            "price_cat_3": [0.3333333333333333, 0.47142436230126994],
            "holiday_stats": [0.044444444444444446, 0.20723493215097805],
            "page_popularity": [3.9093487, 0.7969047]
        }
    }
    duration = model_stats['model']['duration']
    starting_day = datetime.strptime('2018-01-01', '%Y-%m-%d')
    days = [datetime.strftime(starting_day + timedelta(days=i), '%Y-%m-%d') for i in range(duration)]
    x = [7.609366416931152, 4.418840408325195, 4.787491798400879, 4.9972124099731445, 4.584967613220215, 
    4.394449234008789, 5.998936653137207, 6.375024795532227, 4.8903489112854, 4.477336883544922, 
    4.983606815338135, 4.787491798400879, 4.304065227508545, 6.040254592895508, 7.587817192077637, 
    5.176149845123291, 4.477336883544922, 4.8903489112854, 4.934473991394043, 4.875197410583496, 
    5.849324703216553, 6.278521537780762, 4.8978400230407715, 5.2257466316223145, 4.875197410583496, 
    5.24174690246582, 4.7004804611206055, 6.115891933441162, 6.514712810516357, 4.744932174682617, 
    4.905274868011475, 4.955827236175537, 5.036952495574951, 4.770684719085693, 6.079933166503906, 
    6.388561248779297, 5.0434250831604, 5.105945587158203, 5.1704840660095215, 4.682131290435791, 
    5.135798454284668, 6.0450053215026855, 6.398594856262207, 4.72738790512085, 4.007333278656006, 
    4.543294906616211, 5.023880481719971, 4.762174129486084, 6.03308629989624, 7.585280895233154, 
    4.8978400230407715, 4.465908050537109, 4.653960227966309, 4.394449234008789, 4.934473991394043, 
    5.828945636749268, 6.548219203948975, 4.969813346862793, 4.9904327392578125, 4.595119953155518, 
    4.787491798400879, 4.564348220825195, 5.746203422546387, 6.513230323791504, 4.976733684539795, 
    4.510859489440918, 5.003946304321289, 4.430816650390625, 3.828641414642334, 5.902633190155029, 
    6.473890781402588, 4.779123306274414, 4.8903489112854, 4.905274868011475, 5.075173854827881, 
    5.135798454284668, 6.073044300079346, 6.7405195236206055, 5.111987590789795, 4.691348075866699, 
    4.465908050537109, 5.075173854827881, 4.770684719085693, 6.154858112335205, 6.546785354614258, 
    4.7004804611206055, 4.174387454986572, 5.068904399871826, 4.543294906616211, 5.817111015319824]
    full_record = np.round(np.expm1(x))
    
    hours = [i for i in range(0,24)]
    price_categories = [i for i in range(0, 4)]
    records_hour_price_category_list = []
    for hour in hours:
        for price_category in price_categories:
            records_hour_price_category_list.append((full_record, hour, price_category))


    response = predict(TENSORFLOW_SERVICEAPI_URL, model_stats=model_stats, day_list=days, 
                       uckey='magazinelock,1,3G,g_f,2,pt,1004,icc,2,11',
                       age='2', si='1', network='3G', gender='g_f',
                       media='', ip_location='', records_hour_price_list=records_hour_price_category_list)
    print('Returned the response after calling ' + TENSORFLOW_SERVICEAPI_URL)
    print(response)

    predicted_values_list = response[0]

    prediction_window = model_stats['model']['predict_window']
    forward_offset = model_stats['model']['FORWARD_OFFSET']
    final_start = duration - forward_offset # 90 - 11 = 79
    final_end = final_start + prediction_window
    testing_values = full_record[final_start:final_end]
    mape_errors = []
    for i in range(len(predicted_values_list)):
        prediction_hour_price_category = predicted_values_list[i]
        print('\nPrediction: hour ' + str(i // 4) + ', price category ' + str(i % 4 + 1))
        predicted_values = prediction_hour_price_category
        mape_error = round(calculate_MAPE_ERROR(testing_values, predicted_values), 4)
        mape_errors.append(mape_error)
        print("testing values: " + str(testing_values) + "; predicted values: " + str(predicted_values))
        print('MAPE error value based on this prediction: ' + str(mape_error)+ ' (' + str(mape_error * 100) + '%)')
    print('\n\nThe MAPE errors: ' + str(mape_errors))
    print('The average MAPE error: ' + str(round(sum(mape_errors) / len(mape_errors), 4)))

if __name__ == "__main__":
    test_dlpredictor_api()