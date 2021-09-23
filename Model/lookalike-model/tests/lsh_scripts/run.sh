# genrate_user_scores data
spark-submit --master yarn pipeline/genrate_user_scores.py

#user to user similarity calculation using LSH algo
spark-submit --master yarn pipeline/user_similarity_using_LSH_Algo.py