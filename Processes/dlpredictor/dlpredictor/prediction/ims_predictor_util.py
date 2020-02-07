import datetime
from imscommon.model.ucday import UCDay


def get_date(date_y_m_d):
    return datetime.datetime.strptime(date_y_m_d, '%Y-%m-%d')


def get_next_date(date_y_m_d):
    date = get_date(date_y_m_d)
    date += datetime.timedelta(days=1)
    result = date.strftime('%Y-%m-%d')
    return result


def dates_are_same_day_of_week(date1_str, date2_str):
    date1 = get_date(date1_str)
    date2 = get_date(date2_str)
    return date1.weekday() == date2.weekday()


def convert_records_map_to_list(records):
    sorted_records_list = []

    sorted_indices = sorted(records)
    first_date_str = sorted_indices[0]
    last_date_str = sorted_indices[-1]

    # days_str contains all the days from first date to last day
    days_str = []
    days_str.append(first_date_str)

    _date = get_date(first_date_str)
    last_date = get_date(last_date_str)
    while(True):
        if (_date >= last_date):
            break
        _date += datetime.timedelta(days=1)
        _date_str = _date.strftime('%Y-%m-%d')
        days_str.append(_date_str)

    for day_str in days_str:
        if day_str in sorted_indices:
            sorted_records_list.append(records[day_str])
        else:
            sorted_records_list.append(UCDay(day_str))

    return sorted_records_list
