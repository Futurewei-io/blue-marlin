"""
1. Check if the table has partitions or not.
2. Check if the count, distinct count and bucket list health check takes more than timeout time or not

"""
import unittest
from test_base import TestBase


#
# parser = argparse.ArgumentParser(description='Short sample app')
# parser.add_argument("--timeout", default=600)
# args = parser.parse_args()
# timer = int(args.timeout)


def load_df(hive_context, table_name, bucket_id):
    command = """select * from {} where bucket_id = {}""".format(table_name, bucket_id)
    return hive_context.sql(command)


def distinct_count(df, column):
    return df.select(column).distinct().count()

def total_count(df):
    return df.count

def bucket_id_health(df, df2):
    d = df.join(df2, on='uckey', how = 'inner')
    return d.count()

def partition_check(table_name, hive_context):
    try:
        command = """show PARTITIONS {}""".format(table_name)
        hive_context.sql(command)
        return 0
    except Exception:
        return -1

class factdata_health(TestBase):

    def test_partition_check(self):
        result = partition_check(self.table_name, self.hive_context)
        self.assertEqual(result, 0, "passed")

    def test_total_check(self):
        result = self.timer(self.timeout, total_count, args = (self.df))
        self.assertEqual(result, 0, "passed")

    def test_total_distinct_check(self):
        column = 'day'
        result = self.timer(self.timeout,distinct_count, args= (self.df, column) )
        self.assertEqual(result, 0, "passed")

    def test_distinct_count(self):
        column = 'uckey'
        result = self.timer(self.timeout, distinct_count, args =(self.df, column))
        self.assertEqual(result, 0, "passed")

    def test_bucket_check(self):
        result = self.timer(self.timeout, bucket_id_health, args = (self.df, self.df2))
        self.assertEqual(result, 0, "passed")


if __name__ == '__main__':
    unittest.main()

