import pandas as pd
import numpy as np
from sklearn.decomposition import PCA


def main():
    chunk_size = 10000
    print("Applying Principal Component Analysis\n")

    pca = PCA(n_components = 2)
    for df_chunk in pd.read_csv('GerardResults/customer_satisfaction_results.csv', chunksize=chunk_size):
        df_chunk.columns = range(df_chunk.shape[1])
        pca_result = pca.fit_transform(df_chunk[df_chunk.columns[2:]])
        df_chunk['x'] = pca_result[:,0]
        df_chunk['y'] = pca_result[:,1]
        df_chunk = df_chunk[[0, 'x', 'y']]
        df_chunk.to_csv('GerardResults/pca_results.csv', sep=",", mode='a', header=False)



    print("Completed PCA.\n\n")


if __name__ == "__main__":
    main()