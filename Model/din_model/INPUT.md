
Three hive tables are required for din-model with the following schema. 
The name of the tables must match with ones in config.yml

###  persona table
[('did', 'string'), ('gender_new_dev', 'string'), ('forecast_age_dev', 'string')]

###  show-log table
[('did', 'string'), ('adv_id', 'string'), ('adv_type', 'string'), ('slot_id', 'string'), ('spread_app_id', 'string'), ('device_name', 'string'), ('net_type', 'string'), ('adv_bill_mode_cd', 'string'), ('show_time', 'string')]


###  click-log table
[('did', 'string'), ('adv_id', 'string'), ('adv_type', 'string'), ('slot_id', 'string'), ('spread_app_id', 'string'), ('device_name', 'string'), ('net_type', 'string'), ('adv_bill_mode_cd', 'string'), ('click_time', 'string')]
