import pandas as pd
import argparse
import faiss
import numpy as np


def get_numpy_array(read_path):
    arr = np.load(read_path)
    arr = arr.astype('float32')
    return arr


def split_dataframe(df, chunk_size=1000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--numpy_data", type=str, help='user embedding data in numpy format')
    parser.add_argument("--num_neighbors", type=int, default=100, help='number of neighbours')
    parser.add_argument("--saved_dir", type=str, help='directory to save generated tfserving model.')


    args, unknown = parser.parse_known_args()
    if len(unknown) != 0:
        print("unknown args:%s", unknown)

    data_dir = args.numpy_data
    print("data_dir:  ", data_dir)
    num_neighbors = args.num_neighbors
    print("num_neighbors:  ", num_neighbors)
    saved_dir = args.saved_dir+"/"
    print("saved_dir:  ", saved_dir)

    print("reading data files")
    df = get_numpy_array(data_dir)

    no_gpus = faiss.get_num_gpus()
    print("num of gpus available count:  ", no_gpus)

    vector_dim = df.shape[1]
    cpu_index = faiss.IndexFlatL2(vector_dim)

    # build the index
    gpu_index = faiss.index_cpu_to_all_gpus(cpu_index)

    # add vectors to the index
    gpu_index.add(df)
    print("Total num of records indexed: ", gpu_index.ntotal)

    D, I = gpu_index.search(df, num_neighbors + 1)  # actual search
    extended_user = np.squeeze(I)
    extended_user_dist = np.squeeze(D)
    data = {}
    for i in range(num_neighbors + 1):
        data["user" + str(i)] = extended_user[:, i]
        if i > 0:
            data["score" + str(i)] = 1-extended_user_dist[:, i]

    new_df = pd.DataFrame(data)

    new_df = split_dataframe(new_df, chunk_size=10000)
    counter = 0
    for chunk in new_df:
        counter = counter + 1
        chunk.to_csv(saved_dir + f'user_similarity_{str(counter)}.csv', index=False)

    print("job is completed")


