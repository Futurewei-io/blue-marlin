import unittest

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
from test_base import TestBase


class TestCleanPersona(TestBase):

    """ These tests are to verify the din-persona-temp-<DATE> table that is created by main_clean.py"""

    def test_if_records_are_distinct(self):
        command = "select * from {}".format(self.persona_table_name)
        df = self.hive_context.sql(command)
        self.assertEqual(df.count(), df.groupBy(
            'did', 'age', 'gender').count().count())

    def test_if_schema_correct(self):
        schema = [('did', 'string'), ('gender', 'int'), ('age', 'int')]
        command = "select * from {}".format(self.persona_table_name)
        df = self.hive_context.sql(command)
        self.assertEqual(df.dtypes, schema)

    def test_if_a_did_not_associated_with_multiple_gender_age(self):
        command = "select * from {}".format(self.persona_table_name)
        df = self.hive_context.sql(command)
        for item in ['age', 'gender']:
            num_of_did = df.groupBy('did', item).count().agg(
                sum('count')).take(1)[0]['sum(count)']
            self.assertEqual(df.groupBy(
                'did', item).count().count(), num_of_did)


if __name__ == "__main__":
    unittest.main()
