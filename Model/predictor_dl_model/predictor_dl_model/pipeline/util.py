import datetime
import math

def get_dow(day_list):
    dow_list = []
    for day in day_list:
        dow = datetime.datetime.strptime(day, '%Y-%m-%d').weekday()
        dow_list.append(dow)

    week_period = 7.0 / (2 * math.pi)
    sin_list = [math.sin(x / week_period) for x in dow_list]
    cos_list = [math.cos(x / week_period) for x in dow_list]
    return (sin_list, cos_list)



