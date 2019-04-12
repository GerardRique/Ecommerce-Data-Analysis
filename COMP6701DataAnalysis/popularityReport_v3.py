import pandas as pd
import gzip
import matplotlib.pyplot as plt
import numpy as np
from ProductReviewStatistic import ProductReviewStatistic
import heapq
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import csv
import time


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

def get_statistics(path, product_price_dict, product_cat_dict):
    mean_reviews = {}
    statistics_dict = {}
    user_num_reviews_dict = {}
    user_num_helpful_dict = {}
    all_product_rating_hist = [0, 0, 0, 0, 0]
    num_reviews = 0
    mean_review = 0
    highest_price = 0.0
    cheapest_price = 99999.0
    expensive_high_review_item = ""
    cheap_high_review_item = ""
    category_list = []
    customer_satisfaction_dict = {}
    for d in parse_file(path):
        try:
            all_product_rating_hist[round(int(d['overall']))] += 1
            num_reviews += 1
            mean_review = mean_review + ((int(d['overall']) - mean_review) / num_reviews)
        except Exception:
            pass 

        try:
            if (int(d['overall']) >= 4) and (float(product_price_dict[d['asin']]) > highest_price): 
                expensive_high_review_item = d['asin']
                highest_price = float(product_price_dict[d['asin']])
            
            if (int(d['overall']) >= 4) and (float(product_price_dict[d['asin']]) < cheapest_price):
                cheap_high_review_item = d['asin']
                cheapest_price = float(product_price_dict[d['asin']])

        except Exception:
            pass 

        try:
            category = product_cat_dict[d['asin']][0]
            if category not in category_list:
                category_list.append(category)

            index = category_list.index(category)

            if d['reviewerID'] in customer_satisfaction_dict:
                if customer_satisfaction_dict[d['reviewerID']] < category_list:
                    difference = len(category_list) - len(customer_satisfaction_dict[d['reviewerID']])
                    section = [0] * difference
                    customer_satisfaction_dict[d['reviewerID']].extend(section)
                customer_satisfaction_dict[d['reviewerID']][index] += 1
            else:
                customer_satisfaction_dict[d['reviewerID']] = [0] * len(category_list)
                customer_satisfaction_dict[d['reviewerID']][index] += 1
        except Exception:
            pass

        if d['asin'] in mean_reviews:
            current_mean = mean_reviews[d['asin']][0]
            mean_reviews[d['asin']][1] += 1
            mean_reviews[d['asin']][0] = round((current_mean + ((int(d['overall']) - current_mean) / mean_reviews[d['asin']][1])))
            statistics_dict[d['asin']][round(int(d['overall'])) - 1] += 1
        else:
            mean_reviews[d['asin']] = [int(d['overall']), 1]
            statistics_dict[d['asin']] = [0, 0, 0, 0, 0]
            statistics_dict[d['asin']][round(int(d['overall'])) - 1] += 1

        try:
            helpful_rating = d['helpful'][0]/(d['helpful'][0] + d['helpful'][1])
            if d['reviewerID'] in user_num_reviews_dict:
                user_num_reviews_dict[d['reviewerID']] += 1
                user_num_helpful_dict[d['reviewerID']] += d['helpful'][0]

            else:
                user_num_reviews_dict[d['reviewerID']] = 1
                user_num_helpful_dict[d['reviewerID']] = d['helpful'][0]
        except Exception:
            pass


    return mean_reviews, statistics_dict, all_product_rating_hist, mean_review, user_num_reviews_dict, expensive_high_review_item, highest_price, cheap_high_review_item, cheapest_price, user_num_helpful_dict, category_list, customer_satisfaction_dict

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



def main():

    product_price_dict = {}
    product_cat_dict = {}

    start_time = time.time()

    print("Product File Read: Initiated...\n")
    product_price_dict, product_cat_dict = read_product_file('../AmazonDataset/meta_Musical_Instruments.json.gz', product_price_dict, product_cat_dict)
    print("Product File Read: Completed\n")


    print("Reviewwww File Read: Inititated\n")
    mean_reviews, statistics_dict, all_product_rating_hist, mean_review, user_num_reviews_dict, expensive_high_review_item, highest_price, cheap_high_review_item, cheapest_price, user_num_helpful_dict, category_list, customer_satisfaction_dict = get_statistics('../AmazonDataset/reviews_Musical_Instruments.json.gz', product_price_dict, product_cat_dict)
    print("Review File Read: Completed\n")

    statistics_results_file = open("GerardResults/StatisticsResults.csv", "w")
    statistics_results_file.write("ProductID,Mean,Mode\n")
    #The max_num_reviews product keeps the highest number of reviews that was given to any product
    max_num_reviews = 0
    #The max_num_reviews_prod is the ID for the product with the highest number of reviews
    max_num_reviews_prod = ""
    for key, value in mean_reviews.items():

        if value[1] > max_num_reviews:
            max_num_reviews = value[1]
            max_num_reviews_prod = key

        stats_list = statistics_dict[key]
        statistics_results_file.write(key + "," + str(value[0]) + "," + str(stats_list.index(max(stats_list)) + 1) + "\n")

    statistics_results_file.close()
    print("Statistic Result file write Completed\n")

    skewness_result_file = open("GerardResults/SkewnessResult.txt", "w")
    mode_review = all_product_rating_hist.index(max(all_product_rating_hist)) + 1
    if mode_review == mean_review:
        skewness_result_file.write("Data has normal distribution\n")
    elif mean_review < mode_review:
        skewness_result_file.write("Data is negatively skewed\n")
    else: skewness_result_file.write("Data is positively skewed\n")

    skewness_result_file.close()
    print("Skewness Result file write Completed\n")
    #find the largest value in the user_num_reviews_dict will be the highest number of reviews. The corresponsing key will be the user with the highest number of reviews.
    user_highest_num_reviews = max(user_num_reviews_dict.keys(), key=(lambda key: user_num_reviews_dict[key]))
    user_most_helpful = max(user_num_helpful_dict.keys(), key=(lambda key: user_num_helpful_dict[key]))
    general_stats_file = open("GerardResults/GeneralStatistics.txt", "w")
    general_stats_file.write("Product with most reviews: " + max_num_reviews_prod + "\nNumber of reviews: " + str(max_num_reviews) + "\n\n")
    general_stats_file.write("User with highest number of reviews: " + user_highest_num_reviews + "\nNumber of reviews: " + str(user_num_reviews_dict[user_highest_num_reviews]) + "\n\n")
    general_stats_file.write("User with most helpful reviews: " + user_most_helpful + "\nNumber of helpful reviews: " + str(user_num_helpful_dict[user_most_helpful]) + "\n\n")
    general_stats_file.write("Most expensive high review item: " + expensive_high_review_item + "\nPrice: " + str(highest_price) + "\n\n")
    general_stats_file.write("Cheapest high review item: " + cheap_high_review_item + "\nPrice: " + str(cheapest_price) + "\n")
    general_stats_file.close()
    print("General Statistics file write complete\n\n")

    
    
    # print("Converting data to data frame...\n\n")
    # #Format customer satisfaction dictionary as data frame
    # customer_satisfaction_df = pd.DataFrame.from_dict(customer_satisfaction_dict, orient='index')
    
    # customer_satisfaction_df.fillna(0, inplace=True)
    # cols = customer_satisfaction_df.columns
    # print("Dataframe conversion complete\n")
    print("Creating customer satisfaction file...\n\n")
    with open("GerardResults/customer_satisfaction_results.csv", "w") as outfile:
        writer = csv.writer(outfile, delimiter=",")
        for key, value in customer_satisfaction_dict.items():
            data = [key] + value + [0] * (len(category_list) - len(value))
            writer.writerow(data)

    print("Customer satisfaction file complete\n\n")


    # cluster = KMeans(n_clusters = 5)
    # customer_satisfaction_df['cluster'] = cluster.fit_predict(customer_satisfaction_dict)
    
    # print("Applying Principal component analysis...\n")
    # pca = PCA(n_components = 2)
    # pca_result = pca.fit_transform(customer_satisfaction_df[cols])
    # customer_satisfaction_df['x'] = pca_result[:,0]
    # customer_satisfaction_df['y'] = pca_result[:,1]
    # customer_satisfaction_df = customer_satisfaction_df.reset_index()
    # #customer_satisfaction_df.to_csv('GerardResults/customerSatisfaction.csv', sep=',', encoding='utf-8')
    # customer_satisfaction_df = customer_satisfaction_df[["index", "x", "y"]]
    # customer_satisfaction_df.to_csv('GerardResults/PCAResults.csv', sep=',', encoding='utf-8')
    # print("Principal component analysis completed\n\n")

    

    end_time = time.time()
    time_file = open("ExecutionTime.txt", "w")
    time_length = end_time - start_time
    time_file.write("Time for execution: " + str(time_length) + "\n")
    time_file.close()

    

if __name__ == '__main__':
    main()









