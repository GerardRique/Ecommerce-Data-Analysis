import pandas as pd
import gzip
import matplotlib.pyplot as plt
import numpy as np
from ProductReviewStatistic import ProductReviewStatistic

def parse_file(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield eval(l)
 
def get_data_frame(path, limit):
    i = 0
    data_frame = {}
    for d in parse_file(path):
        if i >= limit:
            break
        data_frame[i] = d
        i += 1
    return pd.DataFrame.from_dict(data_frame, orient='index')

def get_statistics(path, data_set, mean_reviews, limit=None):
    i = 0
    for d in parse_file(path):
        if i % 10000 == 0:
            print(i)
        if d['asin'] in data_set:
            curr_mean_review = data_set[d['asin']].increment_average(d['overall'])
            mean_reviews[d['asin']] = curr_mean_review
        else:
            data_set[d['asin']] = ProductReviewStatistic(d['asin'])
            curr_mean_review = data_set[d['asin']].increment_average(d['overall'])
            mean_reviews[d['asin']] = curr_mean_review
        i+= 1
    #print(data_set)
    return mean_reviews



reviews_data_frame = get_data_frame('../AmazonDataset/aggressive_dedup.json.gz', 100)
data_set = {}
mean_reviews = {}
mean_reviews = get_statistics('../AmazonDataset/aggressive_dedup.json.gz', data_set, mean_reviews, 15000)


# for key, data in mean_reviews.items():
#     print(key, data)
#for index, row in reviews_data_frame.iterrows():
    #print(row['reviewerID'], row['asin'], row['overall'])

