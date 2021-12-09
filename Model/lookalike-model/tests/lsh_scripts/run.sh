# genrate_user_scores data
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 8G --driver-memory 8G genrate_user_scores.py

#user to user similarity calculation using LSH algo
spark-submit --master yarn --num-executors 10 --executor-cores 5 --executor-memory 16G --driver-memory 16G user_similarity_using_LSH_Algo.py