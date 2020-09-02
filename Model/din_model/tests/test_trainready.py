import unittest

import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from test_base import TestBase


class TestTrainReady(TestBase):

    """ These tests are to verify the din-trainready-<DATE> table that is created by main_processing.py"""

    def __check_index_size(self, column):
        size1 = self.df_tr.select(column + '_index').distinct().count()
        size2 = self.df_tr.select(column).distinct().count()
        self.assertEqual(size1, size2)

    @unittest.skip("tested")
    def test_uckey_index_size_is_correct(self):
        self.__check_index_size('uckey')

    @unittest.skip("tested")
    def test_media_category_index_size_is_correct(self):
        self.__check_index_size('media_category')

    def __check_index_matching(self, column_name):

        # DataFrame[interval_starting_time: array<int>, keywords: array<string>,
        # click_counts: array<string>, show_counts: array<string>, uckey_index: int,
        # media_index: int, media_category_index: int, net_type_index: int,
        # gender_index: int, age_index: int, region_id_index: int]

        index_name = column_name + '_index'
        df = self.df_tr
        df = df.select(column_name, index_name).distinct()
        df = df.groupBy(column_name).agg(f.collect_set(index_name).alias('cs'))
        df = df.withColumn('count', f.udf(
            lambda x: len(x), IntegerType())('cs'))
        count = df.filter('count > 1').count()
        self.assertTrue(count == 0)

    @unittest.skip("tested")
    def test_uckey_index_mathcing(self):
        self.__check_index_matching('uckey')

    @unittest.skip("tested")
    def test_media_category_index_matching(self):
        self.__check_index_matching('media_category')

    @unittest.skip("tested")
    def test_age_index_matching(self):
        self.__check_index_matching('age')

    @unittest.skip("tested")
    def test_trainready_schema(self):
        """
        Check the fields of train_ready accoording to requirements.
        """

        required = [('age', 'string'), ('age_index', 'int'), ('gender', 'string'),
                    ('gender_index', 'int'), ('interval_starting_time', 'array<int>'),
                    ('keyword_indexes',
                     'array<string>'), ('keyword_indexes_click_counts', 'array<string>'),
                    ('keyword_indexes_show_counts',
                     'array<string>'), ('keywords', 'array<string>'),
                    ('keywords_click_counts',
                     'array<string>'), ('keywords_show_counts', 'array<string>'),
                    ('media', 'string'), ('media_category',
                                          'string'), ('media_category_index', 'int'),
                    ('media_index', 'int'), ('net_type',
                                             'string'), ('net_type_index', 'int'),
                    ('region_id', 'string'), ('region_id_index',
                                              'int'), ('uckey', 'string'),
                    ('uckey_index', 'int')]

        self.assertTrue(sorted(self.df_tr.dtypes) == sorted(required))

    @unittest.skip("tested")
    def test_trainready_distinct_uckey_row(self):
        """
        Check if every row is corresponded into only one uckey.
        """
        ucdoc_attr = ['age', 'gender', 'media_category',
                      'media', 'net_type', 'region_id']
        ucdoc_attr_index = [item+'_index' for item in ucdoc_attr]
        df1 = self.df_tr.select(ucdoc_attr_index).distinct()
        df2 = self.df_tr.select('uckey_index').distinct()
        df3 = self.df_tr.select('uckey').distinct()
        self.assertTrue(df1.count() == df2.count())
        self.assertTrue(df1.count() == self.df_tr.count())
        self.assertTrue(df3.count() == self.df_tr.count())

    def __convert_keyword_count_to_map(self, kcstr):
        """
        kcstr = [<keyword>:<count>]
        kcstr = '29:1,26:3,25:8'
        """
        result = {}
        items = kcstr.split(',')
        for item in items:
            parts = item.split(':')
            if parts[0] in result:
                self.assertTrue(False)
            count = int(parts[1])
            result[parts[0]] = count
        return result

    def __build_count_from_log(self, uckey, interval_starting_time, is_click, keywords):
        df = self.df_logs.where('uckey == "{}" and interval_starting_time == {} and is_click == {}'.format(
            uckey, interval_starting_time, is_click))
        records = df.groupBy('keyword').agg(
            f.count(f.lit(1)).alias('count')).collect()
        result = {}
        for record in records:
            result[record['keyword']] = int(record['count'])
        for keyword in keywords.split(','):
            if keyword not in result:
                result[keyword] = 0
        return result

    def __check_uckey_keyword_and_counts(self, uckey):
        record = self.df_tr.where('uckey == "{}"'.format(uckey)).collect()[0]
        time_intervals = record['interval_starting_time']
        for i, interval_starting_time in enumerate(time_intervals):
            print('test counts uckey="{}", interval_starting_time={}'.format(
                uckey, interval_starting_time))
            clicks = record['keywords_click_counts'][i]
            shows = record['keywords_show_counts'][i]
            keywords = record['keywords'][i]
            click_count_map = self.__convert_keyword_count_to_map(clicks)
            show_count_map = self.__convert_keyword_count_to_map(shows)

            # build show_count_map from logs and compare
            logs_count_map = self.__build_count_from_log(uckey=uckey,
                                                         interval_starting_time=interval_starting_time, is_click=0, keywords=keywords)
            is_error = show_count_map == logs_count_map
            if not is_error:
                print(show_count_map, logs_count_map, keywords)
                self.assertTrue(show_count_map == logs_count_map,
                                msg='keyword count error for uckey="{}", interval_starting_time={}'.format(uckey, interval_starting_time))

            logs_count_map = self.__build_count_from_log(uckey=uckey,
                                                         interval_starting_time=interval_starting_time, is_click=1, keywords=keywords)
            is_error = click_count_map == logs_count_map
            if not is_error:
                print(click_count_map, logs_count_map, keywords)
                self.assertTrue(click_count_map == logs_count_map,
                                msg='click count error for uckey="{}", interval_starting_time={}'.format(uckey, interval_starting_time))

    def test_trainready_have_correct_keyword_and_counts(self):
        """
        This method find 5 most populated uckeys (with the longest time interval) and
        checks their keywords and their impressions and their clicks againt log table.
        """
        rows = self.df_tr.withColumn('length', f.udf(lambda x: len(x), IntegerType())(self.df_tr.interval_starting_time)).select(
            'length', 'uckey').filter('length>1').orderBy('length', ascending=False).take(5)
        for row in rows:
            uckey = row['uckey']
            self.__check_uckey_keyword_and_counts(uckey)


if __name__ == "__main__":
    unittest.main()
