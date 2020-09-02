import unittest
import sys
import yaml

from pyspark import SparkContext, SparkConf, Row
from pyspark.sql.functions import concat_ws, count, lit, col, udf, expr, collect_list, explode, sum
from pyspark.sql import HiveContext


class TestBase(unittest.TestCase):

    """ These tests are to verify the din-trainready-<DATE> table that is created by main_processing.py"""

    @classmethod
    def setUpClass(cls):
        sc = SparkContext()
        sc.setLogLevel('warn')
        cls.hive_context = HiveContext(sc)

        with open('config.yml', 'r') as ymlfile:
            cfg = yaml.load(ymlfile)

        cfg_clean = cfg['pipeline']['main_clean']
        cls.persona_table_name = cfg_clean['data_output']['persona_output_table_name']
        cls.show_table_name = cfg_clean['data_output']['showlog_output_table_name']
        cls.click_table_name = cfg_clean['data_output']['clicklog_output_table_name']

        cfg_main_trainready = cfg['pipeline']['main_trainready']
        trainready_table_name = cfg_main_trainready['trainready_output_table_name']

        cfg_main_logs = cfg['pipeline']['main_logs']
        logs_table_name = cfg_main_logs['logs_output_table_name']

        command = "select * from {}".format(logs_table_name)
        cls.df_logs = cls.hive_context.sql(command)
        
        command = "select * from {}".format(trainready_table_name)
        cls.df_tr = cls.hive_context.sql(command)
