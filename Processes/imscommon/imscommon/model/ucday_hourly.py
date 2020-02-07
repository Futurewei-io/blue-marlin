class UCDay_Hourly:

    def __init__(self, ucday):
        self.date = ucday.date
        self.hourly_map = {}
        for i in range(0, 24):
            self.hourly_map['hr'+str(i)] = ucday.hours[i]
