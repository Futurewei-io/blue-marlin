sql('CREATE TABLE IF NOT EXISTS ads_persona_0520 (did string, gender_new_dev string,forecast_age_dev string)')
sql('CREATE TABLE IF NOT EXISTS din_ad_keywords_09172020 (keyword string, spread_app_id string, keyword_index int)')
sql('CREATE TABLE IF NOT EXISTS ads_clicklog_0520 (did string, adv_id string, adv_type string, slot_id string, spread_app_id string, device_name string, net_type string, adv_bill_mode_cd string, click_time string)')
sql('CREATE TABLE IF NOT EXISTS ads_showlog_0520 (did string, adv_id string, adv_type string, slot_id string, spread_app_id string, device_name string, net_type string, adv_bill_mode_cd string, show_time string)')
sql('CREATE TABLE IF NOT EXISTS din_persona_09172020 (did string, gender int, age int)')
sql('CREATE TABLE IF NOT EXISTS  ads_showlog_0520 (did string, adv_id string, adv_type string, slot_id string, spread_app_id string, device_name string, net_type string, adv_bill_mode_cd string, show_time string)')
sql('CREATE TABLE IF NOT EXISTS  din_showlog_09172020 (did string, adv_id string, slot_id string, spread_app_id string, device_name string, net_type string, price_model string, action_time string, media string, media_category string, gender integer, age integer, keyword string, keyword_index integer)')
sql('CREATE TABLE IF NOT EXISTS  din_clicklog_09172020 (did string, adv_id string, slot_id string, spread_app_id string, device_name string, net_type string, price_model string, action_time string, media string, media_category string, gender integer, age integer, keyword string, keyword_index integer)')
sql('CREATE TABLE IF NOT EXISTS  din_logs_09172020 (did string, is_click integer, action_time string, keyword string, keyword_index integer, media string, media_category string, net_type string, gender integer, age integer, adv_id string, action_time_seconds integer, interval_starting_time integer, uckey string, region_id integer)')
sql('CREATE TABLE IF NOT EXISTS din_trainready_09172020 (region_id integer, age integer, gender integer, net_type string, media_category string, media string, uckey string, interval_starting_time array<integer>, keyword_indexes array<string>, keyword_indexes_click_counts array<string>, keyword_indexes_show_counts array<string>, keywords array<string>, keywords_click_counts array<string>, keywords_show_counts array<string>, uckey_index integer, media_index integer, media_category_index integer, net_type_index integer, gender_index integer, age_index integer, region_id_index integer)')
sql('insert into din_persona_09172020 values ("1001", 1, 2), ("1002", 0, 3)')
sql("insert into din_logs_09172020 values ('3eca6372fdb5a0ababd8f9bc3bf4f241dbe39ce1546ea3efb8efdce172a9fe22', 0, '2020-04-11 07:02:26.168', 'reading', 24, 'native', 'Huawei Browser', '4G', 0, 3, '45034109', 1586613746, 1586563200, 'native,Huawei Browser,4G,0,3,81', 81), ('3eca6372fdb5a0ababd8f9bc3bf4f241dbe39ce1546ea3efb8efdce172a9fe22', 0, '2020-04-11 01:07:39.889', 'reading', 24, 'native', 'Huawei Browser', '4G', 0, 3, '45033921', 1586592459, 1586563200, 'native,Huawei Browser,4G,0,3,81', 81), ('44927efedec6d9353f5122e6566d12161ff67edaab9eff35df61a86076f0a58e', 0, '2020-04-14 06:20:42.377', 'reading', 24, 'native', 'Huawei Browser', '4G', 0, 3, '45033921', 1586870442, 1586822400, 'native,Huawei Browser,4G,0,3,81', 81), ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, '2020-04-08 20:29:20.114', 'video', 29, 'roll', 'Huawei Video', '4G', 0, 4, '45030499', 1586402960, 1586390400, 'roll,Huawei Video,4G,0,4,82', 82), ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, '2020-04-13 12:20:30.899', 'video', 29, 'roll', 'Huawei Video', '4G', 0, 4, '45034378', 1586805630, 1586736000, 'roll,Huawei Video,4G,0,4,82', 82), ('edaa1f204c170d7dca68cd5d60915507feb76fff91faf49a65251106b1f517db', 0, '2020-04-09 00:47:01.142', 'video', 29, 'roll', 'Huawei Video', '4G', 0, 4, '45030868', 1586418421, 1586390400, 'roll,Huawei Video,4G,0,4,82', 82), ('d5c75d1a92f0531db61d39cc5a5fe5c6d98d892327b145ae4fcc74222a39029e', 0, '2020-04-11 18:59:30.550', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 1, 3, '45024950', 1586656770, 1586649600, 'splash,Huawei Reading,WIFI,1,3,51', 51), ('d5c75d1a92f0531db61d39cc5a5fe5c6d98d892327b145ae4fcc74222a39029e', 0, '2020-04-01 18:34:25.112', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 1, 3, '45025925', 1585791265, 1585785600, 'splash,Huawei Reading,WIFI,1,3,51', 51), ('3a8b7621165f117eccdf6137d679dad5fd4ef89ae0f023aa75a60d3520725580', 0, '2020-03-28 11:06:47.168', 'video', 29, 'splash', 'Huawei Reading', 'WIFI', 1, 3, '45027548', 1585418807, 1585353600, 'splash,Huawei Reading,WIFI,1,3,51', 51)")

