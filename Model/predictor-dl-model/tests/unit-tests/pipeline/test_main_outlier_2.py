#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col, udf, collect_set
from pyspark.sql.types import IntegerType, StructField, StructType, ArrayType
from predictor_dl_model.pipeline import main_outlier_2


class TestMainOutlier2(unittest.TestCase):

    def setUp(self):
        # Initialize the Spark session
        self.spark = SparkSession.builder.appName('unit test').enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')

    def compare_list(self, list1, list2):

        if len(list1) != len(list2):
            return False

        def _key(x): return '-'.join([str(_) for _ in x])
        return sorted(list1, key=_key) == sorted(list2, key=_key)

    def test_clean_persona(self):
        ts_1 = [41581, 47933, 2337, 31674, 2026, 2213, 1905, 2056, 2194, 2699, 1915, 2338, 1793, 1720, 2376, 1925, 1767, 1646, 1770, 1872, 1605, 1970, 2106, 2293, 2066, 2302, 2384, 2564, 3013, 3715, 3237, 2579, 2462, 2466, 2536, 2686, 3413, 2902, 2752, 2949, 2843, 3080, 2625, 3533, 2859, 2221, 2577, 2515, 2564, 2645, 3391, 2972, 2614, 2242, 2526, 2406,
                2551, 2829, 3094, 2430, 2426, 2865, 2597, 2189, 3055, 2739, 3119, 2487, 2486, 2418, 2366, 3330, 2056, 2477, 2297, 2492, 2738, 3497, 3459, 2749, 2762, 2775, 2740, 2360, 2456, 2256, 2338, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2496, 2893, 2597, 2996, 2621, 2649, 2794, 2940, 2479, 2362, 2517, 2933, 2908, 0, 2867, 2991, 3578, 3370, 2978, 2843]
        ts_2 = [49284018, 49980296, 49720390, 48817005, 46188827, 45220080, 45218926, 46320152, 48115833, 42565137, 46274392, 46550370, 47304652, 48735907, 53543657, 51825761, 45674027, 46378977, 47456316, 46608388, 48902464, 53834628, 53072749, 47915134, 48711636, 3552366, 47953668, 50043816, 53384695, 52418182, 48548480, 51430554, 3570249, 50135208, 50773416, 54109904, 53145272, 48993429, 49859118, 28682359,
                50162947, 51417061, 56082278, 58081202, 56587454, 49653692, 50419992, 51264370, 51357623, 55675744, 54140460, 48681087, 49139782, 49027937, 49702184, 51606529, 54272728, 53950265, 50946483, 50770559, 52674935, 53625566, 54932789, 53708823, 52607617, 53570029, 53920060, 54071485, 55147557, 56641596, 19696093, 19326623, 20354575, 19264565, 18380537, 21243467, 20715587, 21518251, 19611660, 17768508]
        ts_3 = [49284018, 49980296, 49720390, 48817005, 46188827, 45220080, 45218926, 46320152, 48115833, 42565137, 46274392, 46550370, 47304652, 48735907, 53543657, 51825761, 45674027, 46378977, 47456316, 46608388, 48902464, 53834628, 53072749, 47915134, 48711636, 3552366, 47953668, 50043816, 53384695, 52418182, 48548480, 51430554, 3570249, 50135208, 50773416,
                54109904, 53145272, 48993429, 49859118, 28682359, 50162947, 51417061, 56082278, 58081202, 56587454, 49653692, 50419992, 51264370, 51357623, 55675744, 54140460, 48681087, 49139782, 49027937, 49702184, 51606529, 54272728, 53950265, 50946483, 50770559, 52674935, 53625566, 54932789, 53708823, 52607617, 53570029, 53920060, 54071485, 55147557, 56641596]

        data = [
            (ts_1,), (ts_2,), (ts_3,),
        ]
        schema = StructType([
            StructField("ts", ArrayType(IntegerType()), True)
        ])

        df = self.spark.createDataFrame(data, schema)
        df = main_outlier_2.run_outlier(df, 18, 6)

        df.show(10, False)

        ts_1_expected = [0, 2520, 2520, 2337, 2520, 2025, 2213, 1905, 2056, 2194, 2699, 1915, 2338, 1793, 1720, 2376, 1924, 1767, 1645, 1770, 1871, 1605, 1970, 2106, 2293, 2066, 2302, 2384, 2564, 3013, 3715, 3237, 2579, 2462, 2466, 2536, 2686, 3412, 2902, 2752, 2949, 2843, 3080, 2625, 3533, 2859, 2221, 2577, 2515, 2564, 2645, 3391, 2972, 2614, 2242, 2526, 2406, 2551, 2829, 3094,
                         2430, 2426, 2865, 2597, 2189, 3055, 2739, 3119, 2487, 2486, 2418, 2366, 3330, 2056, 2477, 2297, 2492, 2738, 3497, 3459, 2749, 2762, 2775, 2740, 2360, 2456, 2256, 2338, 2482, 2482, 2486, 2489, 2489, 2489, 2489, 2489, 2489, 2489, 2489, 2489, 2489, 2489, 2496, 2893, 2597, 2996, 2621, 2649, 2794, 2940, 2479, 2362, 2517, 2933, 2908, 2489, 2867, 2991, 3578, 3369, 2978, 2843]
        ts_2_expected = [49284018, 49980296, 49720390, 48817005, 46188827, 45220080, 45218926, 46320152, 48115833, 42565137, 46274392, 46550370, 47304652, 48735907, 53543657, 51825761, 45674027, 46378977, 47456316, 46608388, 48902464, 53834628, 53072749, 47915134, 48711636, 49677938, 47953668, 50043816, 53384695, 52418182, 48548480, 51430554, 49780651, 50135208, 50773416, 54109904, 53145272, 48993429, 49859118,
                         50859949, 50162947, 51417061, 56082278, 58081202, 56587454, 49653692, 50419992, 51264370, 51357623, 55675744, 54140460, 48681087, 49139782, 49027937, 49702184, 51606529, 54272728, 53950265, 50946483, 50770559, 52674935, 53625566, 54932789, 53708823, 52607617, 53570029, 53920060, 54071485, 55147557, 56641596, 50771987, 50771987, 50771987, 50771987, 50771987, 50771987, 50771987, 50771987, 50771987, 50771987]
        ts_3_expected = [49284018, 49980296, 49720390, 48817005, 46188827, 45220080, 45218926, 46320152, 48115833, 42565137, 46274392, 46550370, 47304652, 48735907, 53543657, 51825761, 45674027, 46378977, 47456316, 46608388, 48902464, 53834628, 53072749, 47915134, 48711636, 49677938, 47953668, 50043816, 53384695, 52418182, 48548480, 51430554, 49780651, 50135208,
                         50773416, 54109903, 53145272, 48993429, 49859118, 50859949, 50162947, 51417061, 56082278, 58081202, 56587454, 49653692, 50419992, 51264370, 51357623, 55675743, 54140460, 48681087, 49139782, 49027937, 49702184, 51606529, 54272728, 53950265, 50946483, 50770559, 52674935, 53625566, 54932789, 53708823, 52607617, 53570029, 53920060, 54071485, 55147557, 56641596]
        expected_output = [
            (ts_1_expected,),
            (ts_2_expected,),
            (ts_3_expected,),
        ]

        self.assertTrue(self.compare_list(df.collect(), expected_output))


# Runs the tests.
if __name__ == '__main__':
    # Run the unit tests.
    unittest.main()
