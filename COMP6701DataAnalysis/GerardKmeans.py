import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


def main():
    print("Reading PCA file...\n")
    customer_satisfaction_df = pd.read_csv('GerardResults/pca_results.csv')
    customer_satisfaction_df.columns = range(customer_satisfaction_df.shape[1])
    print("Completed file read.\nInitiating KMeans algorithm...\n")

    cluster = KMeans(n_clusters = 5)
    customer_satisfaction_df['cluster'] = cluster.fit_predict(customer_satisfaction_df[customer_satisfaction_df.columns[2:]])

    print("Completed KMeans algorithm\nWriting data to file...\n")
    customer_satisfaction_df.to_csv('GerardResults/k_means_results.csv', sep=',', encoding='utf-8')

    print("Completed KMeans data file.\n\n")


if __name__ == "__main__":
    main()