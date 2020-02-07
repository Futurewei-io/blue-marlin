from imscommon.model.uchour import UCHour
import collections


class UCDay:
    def __init__(self, date):
        self.hours = []
        self.date = date
        for h_index in range(0, 24):
            self.__add_hour(UCHour(h_index))

    def __add_hour(self, uchour):
        self.hours.append(uchour)

    def update_hour(self, h_index, uchour):
        self.hours[h_index] = uchour

    @staticmethod
    def build(dict):
        object = collections.namedtuple("UCDay", dict.keys())(*dict.values())
        newHours = []
        for hourDict in object.hours:
            obHour = UCHour.build(hourDict)
            newHours.append(obHour)
        ucday = UCDay(object.date)
        ucday.hours = newHours
        return ucday

    @staticmethod
    def add(date, ucday1, ucday2):
        result = UCDay(date)
        for i in range(0, 24):
            sum_uchour = UCHour.add(ucday1.hours[i], ucday2.hours[i])
            result.hours[i] = sum_uchour
        return result

    @staticmethod
    def devide(ucday, length_days):
        result = UCDay(ucday.date)
        for i in range(0, 24):
            _uchour = UCHour.devide(ucday.hours[i], length_days)
            result.hours[i] = _uchour
        return result

    @staticmethod
    def average(date, ucday_list):
        result = UCDay(date)
        for ucday in ucday_list:
            result = UCDay.add(date, result, ucday)
        result = UCDay.devide(result, len(ucday_list))
        return result
