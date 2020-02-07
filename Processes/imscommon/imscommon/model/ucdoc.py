from imscommon.model.ucday import UCDay


class UCDoc:
    uckey_delimiter = ','

    def __init__(self, dataFrameRow):
        if (dataFrameRow != None):
            df_row = dataFrameRow
            self.uckey = df_row.uckey
            self.m = df_row.adv_type
            self.si = df_row.slot_id
            self.t = df_row.net_type
            self.g = df_row.gender
            self.a = df_row.age
            #self.dpc = df_row.price_dev_cat
            self.pm = df_row.pricing_type
            self.r = df_row.residence_city
            self.ipl = df_row.ip_city_code
            self.records = {}
            # self.lastUpdate = None

    @staticmethod
    def build_from_es_doc(dict_ucdoc):
        ucdoc = UCDoc(None)
        ucdoc.uckey = dict_ucdoc.get('uckey')
        ucdoc.m = dict_ucdoc.get('m')
        ucdoc.si = dict_ucdoc.get('si')
        ucdoc.t = dict_ucdoc.get('t')
        ucdoc.a = dict_ucdoc.get('a')
        ucdoc.g = dict_ucdoc.get('g')
        #ucdoc.dpc = dict_ucdoc['dpc']
        ucdoc.pm = dict_ucdoc.get('pm')
        ucdoc.r = dict_ucdoc.get('r')
        ucdoc.ipl = dict_ucdoc.get('ip_city_code')
        ucdoc.records = {}
        # ucdoc.lastUpdate = None

        if dict_ucdoc.get('records') is not None:
            for date, ucday_doc in dict_ucdoc['records'].items():
                ucday = UCDay.build(ucday_doc)
                ucdoc.records[date] = ucday

        return ucdoc

    @staticmethod
    def build_from_concat_string(uckey):
        # extract from reducer._get_key(self, df):
        parts = uckey.split(UCDoc.uckey_delimiter)
        ucdoc = UCDoc(None)
        ucdoc.uckey = uckey
        # ucdoc.lastUpdate = None
        ucdoc.records = {}
        ucdoc.m = parts[0]
        ucdoc.si = parts[1]
        ucdoc.t = parts[2]
        ucdoc.g = parts[3]
        ucdoc.a = parts[4]
        #ucdoc.dpc = parts[5]
        ucdoc.pm = parts[5]
        ucdoc.r = parts[6]
        ucdoc.ipl = parts[7]
        return ucdoc
