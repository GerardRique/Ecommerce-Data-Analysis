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

def get_statistics(path, data_set, mean_reviews, total_reviews, user_most_reviews, customer_satisfaction_df, product_cat_dict, helpful_score_df, limit=None):
    i = 0
    stats_list = [0, 0, 0, 0, 0]
    for d in parse_file(path):
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

        current_rating = int(d['overall'])
        stats_list[current_rating - 1] += 1

        try: 
            number_of_thumbs_up = d['helpful'][0]

            if d['reviewerID'] in user_most_reviews:
                user_most_reviews[d['reviewerID']][0] += 1
                user_most_reviews[d['reviewerID']][1] += number_of_thumbs_up
            else: 
                user_most_reviews[d['reviewerID']] = [1, number_of_thumbs_up]

            helpful_val = d['helpful'][0] / (d['helpful'][0] + d['helpful'][1])
            #helpful_score_df = helpful_score_df.append({'helpful': helpful_val, 'score': int(d['overall'])}, ignore_index=True)
            helpful_score_df = np.append(helpful_score_df, [[helpful_val, int(d['overall'])]], axis=0)
        except Exception:
            pass

        # try:
        #     if d['reviewerID'] in customer_satisfaction_df.index:
        #         categories = product_cat_dict[d['asin']]
        #         for category in categories:
        #             if category in customer_satisfaction_df.columns:
        #                 if np.isnan(customer_satisfaction_df.loc[d['reviewerID'], category]):
        #                     customer_satisfaction_df.loc[d['reviewerID'], category] = 1
        #                 else: customer_satisfaction_df.loc[d['reviewerID'], category] += 1
        #             else:
        #                 customer_satisfaction_df.loc[d['reviewerID'], category] = 1
        #     else:
        #         categories = product_cat_dict[d['asin']]
        #         for category in categories:
        #             customer_satisfaction_df.loc[d['reviewerID'], category] = 1 
                    
        # except Exception:
        #     pass

    return data_set, customer_satisfaction_df, helpful_score_df, stats_list

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
customer_satisfaction_df = pd.DataFrame()
helpful_score_df = np.array([[0, 0], [0, 0]])
print("Product File Read: Initiated...\n")
product_price_dict, product_cat_dict = read_product_file('metadata.json.gz', product_price_dict, product_cat_dict)
print("Product File Read: Completed\n")

#The data set dictionary contains a set of key, value pairs such that each key represents a product ID and each value represents an instance of the ProductReviewStatistic.
data_set = {}
#The mean reviews dictionary contains a set of key value pairs such that each key is a product ID and each value represents the mean review for that product.
mean_reviews = {}
#The total reviews dictionary is one such that each key represents a product ID and each value represents the number of reviews for that product. 
total_reviews = {}
#The user_most_reviews dictionary is one such that each key represents a user ID and each value represents the number of reviews for the corresponding user. 
user_most_reviews = {}
print("Review File Read: Inititated\n")
data_set, customer_satisfaction_df, helpful_score_df, stats_list = get_statistics('aggressive_dedup.json.gz', data_set, mean_reviews, total_reviews, user_most_reviews, customer_satisfaction_df, product_cat_dict, helpful_score_df)
print("Review File Read: Completed\n")

print("Product Statistic File Output: Initiated\n")
stats_result_file = open("GerardStatistics_result.txt", "w")
for key, element in data_set.items():
    stats_result_file.write(element.get_details())

stats_result_file.close()
print("Product Statistic File Output: Completed\n")

print("Skewness Result File Output: Inititated\n")
skewness_file = open("GerardSkewnessResult.txt", "w")
print(stats_list)
for stat in stats_list:
    skewness_file.write(str(stat) + ",")

skewness_file.close()
print("Skewness Result File Output: Completed\n")


print("General Statistics File Output: Inititated\n")
f = open("GerardResults.txt", "w")
#--------------Task A--------------
#--------------Section a ----------

#--------------Section c Part 1: Product that was reviewed the most ----------
most_reviewed_product = find_smallest_value(total_reviews)
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
print("General Statistics File Output: Completed\n")

#--------------Section b -------------
#print(customer_satisfaction_df.columns)
#print(customer_satisfaction_df.columns[1:])
customer_satisfaction_df.fillna(0, inplace=True)
print('Satisfaction Result Output: Initiated\n')
customer_satisfaction_df.to_csv('recomendation.csv', sep=',', encoding='utf-8')
print('Satisfaction Result Output: Completed\n')
cols = customer_satisfaction_df.columns[1:]



#----------------------KMeans----------------------------
# print("KMeans Clustering: Inititated\n")
# cluster = KMeans(n_clusters = 5) 
# customer_satisfaction_df['cluster'] = cluster.fit_predict(customer_satisfaction_df[customer_satisfaction_df.columns[2:]])

# pca = PCA(n_components = 2)
# customer_satisfaction_df['x'] = pca.fit_transform(customer_satisfaction_df[cols])[:,0]
# customer_satisfaction_df['y'] = pca.fit_transform(customer_satisfaction_df[cols])[:,1]
# customer_satisfaction_df = customer_satisfaction_df.reset_index()


# cluster_result_table = customer_satisfaction_df[["index", "cluster", "x", "y"]]

# cluster_result_table.to_csv('Gerard_cluster_results.csv', sep=',',  encoding='utf-8')
# print("KMeans Clustering: Completed\n")


##-----------------cluster 1---------------------
# cluster1 = cluster_result_table[(cluster_result_table.cluster == 0)] 
# cluster1x = cluster1[["x"]]
# cluster1y = cluster1[["y"]]

# cluster2 = cluster_result_table[(cluster_result_table.cluster == 1)] 
# cluster2x = cluster2[["x"]]
# cluster2y = cluster2[["y"]]

# cluster3 = cluster_result_table[(cluster_result_table.cluster == 2)] 
# cluster3x = cluster3[["x"]]
# cluster3y = cluster3[["y"]]

# cluster4 = cluster_result_table[(cluster_result_table.cluster == 3)] 
# cluster4x = cluster4[["x"]]
# cluster4y = cluster4[["y"]]

# cluster5 = cluster_result_table[(cluster_result_table.cluster == 4)] 
# cluster5x = cluster5[["x"]]
# cluster5y = cluster5[["y"]]


# plt.plot(cluster1x, cluster1y, 'x', color='blue')
# plt.plot(cluster2x, cluster2y, 'x', color='red')
# plt.plot(cluster3x, cluster3y, 'x', color='green')
# plt.plot(cluster4x, cluster4y, 'x', color='magenta')
# plt.plot(cluster5x, cluster5y, 'x', color='cyan')
#plt.show()
print("Helpful vs Score Output: Inititated\n")
#helpful_score_df.to_csv('Gerard_helpful_score.csv', sep=',', encoding='utf-8')
np.set_printoptions(suppress=True)
np.savetxt('Gerard_helpful_score.csv', helpful_score_df, delimiter=",")
print("Helpful vs Score Output: Completed\n")


# plt.plot(x_axis_analysis, y_axis_analysis, 'x', color='blue')
# plt.ylabel('Review Score')
# plt.xlabel('Helpfulness rating')
# plt.show()








