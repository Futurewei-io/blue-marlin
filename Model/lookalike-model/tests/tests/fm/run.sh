
# create folders
mkdir deep_fm_model
mkdir tf_logs
mkdir similar_user_data_dir
unzip data.zip

# Data Prepration

summitSparkJob.sh data_prepration_mrs.py --configs config.yml

summitSparkJob.sh config_to_pickel.py --configs config.yml

summitSparkJob.sh --jars spark-tensorflow-connector_2.11-1.15.0.jar tf_records_converter.py --configs config.yml

# Model Trainning
python3 trainer_MTP.py --data_dir  data --model_save_path deep_fm_model --tensorboard_log_dir tf_logs --model_learning_rate 0.001  --num_epochs 1

# Model Prediction
python3 predict_user_embeddings.py --data_dir data --model_save_path deep_fm_model --user_emb_file_path user_emb

# User Similarity calculation
python3 distance_calculation_using_faiss_gpu.py --numpy_data user_emb.npy --num_neighbors 100 --saved_dir similar_user_data_dir

# DMP Tables
summitSparkJob.sh create_dmp_lookalike_tables.py --configs config.yml
