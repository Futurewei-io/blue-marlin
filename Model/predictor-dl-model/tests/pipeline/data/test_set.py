#testing datasets for dl predictor
factdata_columns = ['uckey', 'count_array', 'hour', 'day', 'bucket_id']
factdata_tested = [
    ('banner,b8nyhficur,4G,,,CPM,1,249', ['1:21'], 20, '2020-03-21', 149), 
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,3,28', ['1:2', '3:2'], 20, '2020-03-22', 149), 
    ('banner,k5hz58vpst,4G,,,CPM,2,213', ['2:23', '1:169'], 20, '2020-03-23', 149)
]
factdata_expected_drop_region = [
    ('banner,b8nyhficur,4G,,,CPM,249', ['1:21'], 20, '2020-03-21', 149), 
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', ['1:2', '3:2'], 20, '2020-03-22', 149), 
    ('banner,k5hz58vpst,4G,,,CPM,213', ['2:23', '1:169'], 20, '2020-03-23', 149)
]

region_mapping_columns = ['new', 'old']
region_mapping_columns_renamed = ['ipl_new', 'ipl_old']
region_mapping_tested = [
    (249, 80),
    (28, 28),
    (213, 76)
]
factdata_expected_region_mapped = [
    ('banner,b8nyhficur,4G,,,CPM,80', ['1:21'], 20, '2020-03-21', 149), 
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', ['1:2', '3:2'], 20, '2020-03-22', 149), 
    ('banner,k5hz58vpst,4G,,,CPM,76', ['2:23', '1:169'], 20, '2020-03-23', 149)
]
new_bucket_size = 10
factdata_new_bucket_tested = [
    ('banner,b8nyhficur,4G,,,CPM,80', ['1:21'], 20, '2020-03-21', 149), 
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', ['1:2', '3:2'], 20, '2020-03-22', 250), 
    ('banner,k5hz58vpst,4G,,,CPM,76', ['2:23', '1:169'], 20, '2020-03-23', 301)
]
factdata_new_bucket_expected = [
    ('banner,b8nyhficur,4G,,,CPM,80', ['1:21'], 20, '2020-03-21', 9), 
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', ['1:2', '3:2'], 20, '2020-03-22', 3), 
    ('banner,k5hz58vpst,4G,,,CPM,76', ['2:23', '1:169'], 20, '2020-03-23', 0)
]
factdata_main_ts_tested = factdata_new_bucket_expected
factdata_main_ts_columns = ['uckey', 'price_cat', 'ts', 'a', 'g', 't', 'si', 'r']
factdata_main_ts_expected = [
    ('banner,b8nyhficur,4G,,,CPM,80', '1', [21, None], '', '', '4G', 'b8nyhficur', ''),
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', '1', [None, 2], '3', 'g_m', 'WIFI', 'd3lxq4l6rn', ''),
    ('banner,d3lxq4l6rn,WIFI,g_m,3,CPM,28', '3', [None, 2], '3', 'g_m', 'WIFI', 'd3lxq4l6rn', ''),
    ('banner,k5hz58vpst,4G,,,CPM,76', '2', [None, None], '', '', '4G', 'k5hz58vpst', ''),
    ('banner,k5hz58vpst,4G,,,CPM,76', '1', [None, None], '', '', '4G', 'k5hz58vpst', '')
]
popularity_th = 5
datapoints_min_th = 0.15
factdata_remove_weak_uckeys_columns = factdata_main_ts_columns + ['imp', 'p']
factdata_remove_weak_uckeys_expected = [
    ('banner,b8nyhficur,4G,,,CPM,80', '1', [21, None], '', '', '4G', 'b8nyhficur', '', 21, 10.5),
]

datapoints_th_uckeys = 0.5
popularity_norm = 0.01
factdata_is_spare_uckeys_columns = factdata_main_ts_columns + ['imp', 'p'] + ['sparse']
factdata_is_spare_uckeys_expected = [
    ('banner,b8nyhficur,4G,,,CPM,80', '1', [21, None], '', '', '4G', 'b8nyhficur', '', 21, 10.5, True)
]
median_popularity_of_dense = 2


factdata_cluster_columns = ['uckey', 'price_cat', 'a', 'g', 't', 'si', 'r', 'ts', 'imp', 'p', 'p_n']
a_feature_value_list = ['','1','2','3','4','5','6']
factdata_cluster_columns_ohe_a = factdata_cluster_columns + ['a_', 'a_1', 'a_2', 'a_3', 'a_4', 'a_5', 'a_6']
g_feature_value_list = ['','g_f','g_m','g_x']
factdata_cluster_columns_ohe_g = factdata_cluster_columns + ['g_', 'g_g_f', 'g_g_m', 'g_g_x']
t_feature_value_list = ['UNKNOWN','3G','4G','WIFI','2G']
factdata_cluster_columns_ohe_t = factdata_cluster_columns + ['t_UNKNOWN','t_3G','t_4G','t_WIFI','t_2G']

factdata_cluster_tested = [
    ('native,a47eavw7ex,WIFI,g_f,5,CPC,35', '1', {'5': 1.0}, {'g_f': 1.0}, {'WIFI': 1.0}, 
    {'a47eavw7ex': 1.0}, {'': 1.0}, [2713, 3151, 1627, 2276, 2743, 2726, 2910, 3306, 3983, 3136, 
    3127, 3551, 3664, 3895, 3601, 3702, 3857, 3791, 4033, 3741, 
    3923, 3847, 3887, 3910, 3667, 3537, 3482, 3621, 4027, 3672, 
    3858, 3745, 3885, 4100, 3914, 3888, 3991, 3864, 3814, 3794, 
    3880, 3789, 2724, 3197, 2648, 2362, 2872, 2922, 2755, 2679, 
    2751, 2579, 2571, 2708, 3026, 3107, 2169, 508, 3892, 3185, 
    3336, 3424, 3662, 3781, 3883, 3609, 520, 3704, 3848, 3975, 
    3837, 3850, 3705, 3888, 3865, 3955, 3591, 3340, 3103, 3706, 
    3701, 3488, 3319, 3145, 3136, 2992, 3149, 2876, 2590, 2527], 
    299788, 3330.977783203125, -0.3134732246398926)
]
factdata_cluster_expected_ohe_a = [
    ('native,a47eavw7ex,WIFI,g_f,5,CPC,35', '1', {'5': 1.0}, {'g_f': 1.0}, {'WIFI': 1.0}, 
    {'a47eavw7ex': 1.0}, {'': 1.0}, [2713, 3151, 1627, 2276, 2743, 2726, 2910, 3306, 3983, 3136, 
    3127, 3551, 3664, 3895, 3601, 3702, 3857, 3791, 4033, 3741, 
    3923, 3847, 3887, 3910, 3667, 3537, 3482, 3621, 4027, 3672, 
    3858, 3745, 3885, 4100, 3914, 3888, 3991, 3864, 3814, 3794, 
    3880, 3789, 2724, 3197, 2648, 2362, 2872, 2922, 2755, 2679, 
    2751, 2579, 2571, 2708, 3026, 3107, 2169, 508, 3892, 3185, 
    3336, 3424, 3662, 3781, 3883, 3609, 520, 3704, 3848, 3975, 
    3837, 3850, 3705, 3888, 3865, 3955, 3591, 3340, 3103, 3706, 
    3701, 3488, 3319, 3145, 3136, 2992, 3149, 2876, 2590, 2527], 
    299788, 3330.977783203125, -0.3134732246398926, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0)
]
factdata_cluster_expected_ohe_g = [
    ('native,a47eavw7ex,WIFI,g_f,5,CPC,35', '1', {'5': 1.0}, {'g_f': 1.0}, {'WIFI': 1.0}, 
    {'a47eavw7ex': 1.0}, {'': 1.0}, [2713, 3151, 1627, 2276, 2743, 2726, 2910, 3306, 3983, 3136, 
    3127, 3551, 3664, 3895, 3601, 3702, 3857, 3791, 4033, 3741, 
    3923, 3847, 3887, 3910, 3667, 3537, 3482, 3621, 4027, 3672, 
    3858, 3745, 3885, 4100, 3914, 3888, 3991, 3864, 3814, 3794, 
    3880, 3789, 2724, 3197, 2648, 2362, 2872, 2922, 2755, 2679, 
    2751, 2579, 2571, 2708, 3026, 3107, 2169, 508, 3892, 3185, 
    3336, 3424, 3662, 3781, 3883, 3609, 520, 3704, 3848, 3975, 
    3837, 3850, 3705, 3888, 3865, 3955, 3591, 3340, 3103, 3706, 
    3701, 3488, 3319, 3145, 3136, 2992, 3149, 2876, 2590, 2527], 
    299788, 3330.977783203125, -0.3134732246398926, 0.0, 1.0, 0.0, 0.0)
]
factdata_cluster_expected_ohe_t = [
    ('native,a47eavw7ex,WIFI,g_f,5,CPC,35', '1', {'5': 1.0}, {'g_f': 1.0}, {'WIFI': 1.0}, 
    {'a47eavw7ex': 1.0}, {'': 1.0}, [2713, 3151, 1627, 2276, 2743, 2726, 2910, 3306, 3983, 3136, 
    3127, 3551, 3664, 3895, 3601, 3702, 3857, 3791, 4033, 3741, 
    3923, 3847, 3887, 3910, 3667, 3537, 3482, 3621, 4027, 3672, 
    3858, 3745, 3885, 4100, 3914, 3888, 3991, 3864, 3814, 3794, 
    3880, 3789, 2724, 3197, 2648, 2362, 2872, 2922, 2755, 2679, 
    2751, 2579, 2571, 2708, 3026, 3107, 2169, 508, 3892, 3185, 
    3336, 3424, 3662, 3781, 3883, 3609, 520, 3704, 3848, 3975, 
    3837, 3850, 3705, 3888, 3865, 3955, 3591, 3340, 3103, 3706, 
    3701, 3488, 3319, 3145, 3136, 2992, 3149, 2876, 2590, 2527], 
    299788, 3330.977783203125, -0.3134732246398926, 0.0, 0.0, 0.0, 1.0, 0.0)
]
