import pandas as pd
import gzip
import matplotlib.pyplot as plt
import numpy as np
from ProductReviewStatistic import ProductReviewStatistic
import heapq
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA


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

def get_statistics(path):
    mean_reviews = {}
    for d in parse_file(path):
        if d['asin'] in mean_reviews:
            current_mean = mean_reviews[d['asin']][0]
            mean_reviews[d['asin']][1] += 1
            mean_reviews[d['asin']][0] = round((current_mean + ((int(d['overall']) - current_mean) / mean_reviews[d['asin']][1])), 1)
        else:
            mean_reviews[d['asin']] = [int(d['overall']), 1]


    return mean_reviews

def read_product_file(path, product_price_dict, product_cat_dict):
    for data in parse_file(path):
        try:
            product_price_dict[data['asin']] = data['price']
            product_cat_dict[data['asin']] = data['categories'][0]
        except Exception:
            pass
    return product_price_dict, product_cat_dict

def find_smallest_value(data_dict):
    items = [(-value, key) for key, value in data_dict.items()]
    smallest = heapq.nsmallest(1, items)
    return smallest[0]

def find_smallest_user_data(data_dict):
    most_review_list = [(-value[0], key) for key, value in data_dict.items()]
    helpful_review_list = [(-value[1], key) for key, value in data_dict.items()]

    most_reviews = heapq.nsmallest(1, most_review_list)
    most_helpful = heapq.nsmallest(1, helpful_review_list)

    result = most_reviews + most_helpful

    return result

def most_expensive_high_review(data_set, product_price_dict):
    result_id = ""
    result_price = 0.0
    cheapest_high_review_price = 99999999
    cheapest_high_review_id = ""
    count = 0
    for key, value in data_set.items():
        if value.is_high_review():
            count += 1
            try:
                current_price = product_price_dict[key]
                if current_price > result_price:
                    result_price = current_price
                    result_id = key
                if current_price < cheapest_high_review_price: 
                    cheapest_high_review_price = current_price
                    cheapest_high_review_id = key
            except Exception:
                pass
    return (result_id, result_price, cheapest_high_review_id, cheapest_high_review_price)





def output_data(results):
    f = open("output.txt", "w")
    for review in results:
        f.write(results[review].get_productId() + " " + str(results[review].get_mean()) + "\n")
    f.close()


product_price_dict = {}
product_cat_dict = {}

print("Product File Read: Initiated...\n")
product_price_dict, product_cat_dict = read_product_file('../AmazonDataSet/meta_Musical_Instruments.json.gz', product_price_dict, product_cat_dict)
print("Product File Read: Completed\n")


print("Review File Read: Inititated\n")
mean_reviews = get_statistics('../AmazonDataSet/reviews_Musical_Instruments.json.gz')
print("Review File Read: Completed\n")

mean_results_file = open("Results/MeanResults.txt", "w")

for key, value in mean_reviews.items():
    mean_results_file.write(key + " : " + str(value[0]) + "\n")
mean_results_file.close()









