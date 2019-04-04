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

def get_statistics(path, data_set, mean_reviews, total_reviews, user_most_reviews, customer_satisfaction_df, product_cat_dict, limit=None):
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

        try: 
            number_of_thumbs_up = d['helpful'][0]

            if d['reviewerID'] in user_most_reviews:
                user_most_reviews[d['reviewerID']][0] += 1
                user_most_reviews[d['reviewerID']][1] += number_of_thumbs_up
            else: 
                user_most_reviews[d['reviewerID']] = [1, number_of_thumbs_up]
        except Exception:
            pass

        try:
            if d['reviewerID'] in customer_satisfaction_df.index:
                categories = product_cat_dict[d['asin']]
                for category in categories:
                    if category in customer_satisfaction_df.columns:
                        customer_satisfaction_df.loc[d['reviewerID'], category] += 1
                    else:
                        customer_satisfaction_df.loc[d['reviewerID'], category] = 1
            else:
                categories = product_cat_dict[d['asin']]
                for category in categories:
                    customer_satisfaction_df.loc[d['reviewerID'], category] = 1 
                    
        except Exception:
            pass
        i+= 1
    return data_set, customer_satisfaction_df

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
    print("Count: " + str(count))
    return (result_id, result_price, cheapest_high_review_id, cheapest_high_review_price)





def output_data(results):
    f = open("output.txt", "w")
    for review in results:
        f.write(results[review].get_productId() + " " + str(results[review].get_mean()) + "\n")
    f.close()


product_price_dict = {}
product_cat_dict = {}
customer_satisfaction_df = pd.DataFrame()
product_price_dict, product_cat_dict = read_product_file('../AmazonDataset/meta_Musical_Instruments.json.gz', product_price_dict, product_cat_dict)
print(len(product_price_dict))

#The data set dictionary contains a set of key, value pairs such that each key represents a product ID and each value represents an instance of the ProductReviewStatistic.
data_set = {}
#The mean reviews dictionary contains a set of key value pairs such that each key is a product ID and each value represents the mean review for that product.
mean_reviews = {}
#The total reviews dictionary is one such that each key represents a product ID and each value represents the number of reviews for that product. 
total_reviews = {}
#The user_most_reviews dictionary is one such that each key represents a user ID and each value represents the number of reviews for the corresponding user. 
user_most_reviews = {}
data_set, customer_satisfaction_df = get_statistics('../AmazonDataset/reviews_Musical_Instruments_5.json.gz', data_set, mean_reviews, total_reviews, user_most_reviews, customer_satisfaction_df, product_cat_dict)


f = open("reults.txt", "w")
#--------------Task A--------------
#--------------Section a ----------

#--------------Section c Part 1: Product that was reviewed the most ----------
most_reviewed_product = find_smallest_value(total_reviews)
print(most_reviewed_product)
f.write("Most reviewd Product: " + most_reviewed_product[1] + "\nNumber of reviews: " + str(most_reviewed_product[0]) + "\n\n")

#--------------Section c Part ii: The user that gave the most reviews ----------
# user_with_most_reviews = find_smallest_value(user_most_reviews)
# print(user_with_most_reviews)
result = find_smallest_user_data(user_most_reviews)
user_with_most_reviews = result[0]
f.write("User with most reviews: " + user_with_most_reviews[1] + "\nNumber of reviews: " + str(user_with_most_reviews[0]) + "\n\n")


#--------------Section c Part iii: The user that gave the most useful reviews ----------
user_with_most_helpful_reviews = result[1]
f.write("User with most helpful reviews: " + user_with_most_helpful_reviews[1] + "\nNumber of Thumbs up: " + str(user_with_most_helpful_reviews[0]) + "\n\n")

#--------------Section c Part iv: The most expensive high review product ----------
expensive_result_tuple = most_expensive_high_review(data_set, product_price_dict)
f.write('Most expensive high review product: ' + expensive_result_tuple[0] + "\nPrice: $" + str(expensive_result_tuple[1]) + "\n\nCheapest High review Product: " + expensive_result_tuple[2] + "\nPrice: $" + str(expensive_result_tuple[3]) +"\n\n")

f.close()


#--------------Section b -------------
print(customer_satisfaction_df.shape)
print(customer_satisfaction_df.columns)





