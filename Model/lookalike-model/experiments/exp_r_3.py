from math import sqrt
import time
import numpy as np
import itertools
import heapq
from pyspark.sql import Row


def calculate_similarity(alpha_user_did_score, c2):
    # we don't need did_list in dataframe
    def __helper(did_list, user_score_matrix, top_n_similar_user_list, c1_list):
        user_score_matrix = np.array(user_score_matrix)
        m = user_score_matrix.shape[1]
        alpha_dids, alpha_score_matrix = alpha_user_did_score
        alpha_score_matrix = np.array(alpha_score_matrix)
        cross_mat = np.matmul(user_score_matrix, alpha_score_matrix.transpose())
        similarity_list = np.sqrt(m) - np.sqrt(np.maximum(np.expand_dims(c1_list, 1) + c2 - (2 * cross_mat), 0.0))
        result = []
        for did, cosimilarity, top_n_similar_user in itertools.izip(did_list, similarity_list, top_n_similar_user_list):
            user_score_s = list(itertools.izip(alpha_dids, cosimilarity.tolist()))
            user_score_s.extend(top_n_similar_user)
            user_score_s = heapq.nlargest(top_n_value, user_score_s, key=lambda x: x[1])
            result.append(user_score_s)
        return result
    return __helper


top_n_value = 3
block_user = [Row(did_list=[u'1', u'2'], score_matrix=[[0.10000000149011612, 0.800000011920929, 0.8999999761581421], [
                  0.10000000149011612, 0.800000011920929, 0.8999999761581421]], c1_list=[1.4600000381469727, 1.4600000381469727])]
block_user_did_score = (block_user[0]['did_list'], block_user[0]['score_matrix'])
c2 = np.array(block_user[0]['c1_list'])

user_score_matrix_1_2 = [[0.1, 0.8, 0.9], [0.1, 0.8, 0.9]]
top_n_similar_user_1_2 = [[], []]
c1_list_1_2 = [1.4600000381469727, 1.4600000381469727]
top_n_similar_user_1_2 = calculate_similarity(block_user_did_score, c2)([1, 2], user_score_matrix_1_2, top_n_similar_user_1_2, c1_list_1_2)
# print(top_n_similar_user_1_2)

user_score_matrix_3_4 = [[0.1, 0.8, 0.9], [0.1, 0.8, 0.9]]
top_n_similar_user_3_4 = [[], []]
c1_list_3_4 = [1.4600000381469727, 1.4600000381469727]
top_n_similar_user_3_4 = calculate_similarity(block_user_did_score, c2)([3, 4], user_score_matrix_3_4, top_n_similar_user_3_4, c1_list_3_4)
# print(top_n_similar_user_3_4)

# second block user/cross user
block_user = [Row(did_list=[u'3', u'4'], score_matrix=[[0.10000000149011612, 0.800000011920929, 0.8999999761581421], [
                  0.10000000149011612, 0.800000011920929, 0.8999999761581421]], c1_list=[1.4600000381469727, 1.4600000381469727])]
block_user_did_score = (block_user[0]['did_list'], block_user[0]['score_matrix'])
c2 = np.array(block_user[0]['c1_list'])

top_n_similar_user_1_2 = calculate_similarity(block_user_did_score, c2)([1, 2], user_score_matrix_1_2, top_n_similar_user_1_2, c1_list_1_2)
print(top_n_similar_user_1_2)

top_n_similar_user_3_4 = calculate_similarity(block_user_did_score, c2)([3, 4], user_score_matrix_3_4, top_n_similar_user_3_4, c1_list_3_4)
print(top_n_similar_user_3_4)
