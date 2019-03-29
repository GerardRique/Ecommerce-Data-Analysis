import pandas as pd
import gzip
import matplotlib.pyplot as plt
import numpy as np
from ProductReviewStatistic import ProductReviewStatistic
import heapq

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

def get_statistics(path, data_set, mean_reviews, total_reviews, user_most_reviews, limit=None):
    i = 0
    for d in parse_file(path):
        if i % 10000 == 0:
            print(i)
        if d['asin'] in data_set:
            curr_mean_review = data_set[d['asin']].increment_average(d['overall'])
            data_set[d['asin']].update_median(int(d['overall']))
            mean_reviews[d['asin']] = curr_mean_review
            total_reviews[d['asin']] += 1
        else:
            data_set[d['asin']] = ProductReviewStatistic(d['asin'])
            curr_mean_review = data_set[d['asin']].increment_average(d['overall'])
            data_set[d['asin']].update_median(int(d['overall']))
            total_reviews[d['asin']] = 1
            mean_reviews[d['asin']] = curr_mean_review

        if d['reviewerID'] in user_most_reviews: 
            user_most_reviews[d['reviewerID']] += 1
        else: 
            user_most_reviews[d['reviewerID']] = 1
        i+= 1
    return data_set

def read_product_file(path, product_price_dict):
    for data in parse_file(path):
        try:
            product_price_dict[data['asin']] = data['price']
        except Exception:
            pass
    return product_price_dict

def find_smallest_value(data_dict):
    items = [(-value, key) for key, value in data_dict.items()]
    smallest = heapq.nsmallest(1, items)
    return smallest


def output_data(results):
    f = open("output.txt", "w")
    for review in results:
        f.write(results[review].get_productId() + " " + str(results[review].get_mean()) + "\n")
    f.close()


product_price_dict = {}
product_price_dict = read_product_file('../AmazonDataset/meta_Grocery_and_Gourmet_Food.json.gz', product_price_dict)
print(len(product_price_dict))

#The data set dictionary contains a set of key, value pairs such that each key represents a product ID and each value represents an instance of the ProductReviewStatistic.
data_set = {}
#The mean reviews dictionary contains a set of key value pairs such that each key is a product ID and each value represents the mean review for that product.
mean_reviews = {}
#The total reviews dictionary is one such that each key represents a product ID and each value represents the number of reviews for that product. 
total_reviews = {}
#The user_most_reviews dictionary is one such that each key represents a user ID and each value represents the number of reviews for the corresponding user. 
user_most_reviews = {}
data_set = get_statistics('../AmazonDataset/reviews_Musical_Instruments.json.gz', data_set, mean_reviews, total_reviews, user_most_reviews)

most_reviewed_product = find_smallest_value(total_reviews)

user_with_most_reviews = find_smallest_value(user_most_reviews)

print(most_reviewed_product)
print(len(user_with_most_reviews))
#print(user_with_most_reviews)

# f = open("results.txt", "w")
# f.write(most_reviewed_product)
# f.write("\n")
# f.write(user_with_most_reviews)
# f.close()



