import random
import itertools as it
import heapq
import csv

def key_generator():
    charcters = [chr(random.randint(65, 90)) for x in range(100)]
    for i in it.count():
        yield ''.join(random.sample(charcters, 3))

def sort_heapq(my_dict):
    items = [(-value, key) for key, value in my_dict.items()]
    smallest = heapq.nsmallest(1, items)
    return [-value for value, key in smallest]


categories = ["Music", "sport", "clothing", "electronics", "education"]

data_dict = {}

data_dict["A1000"] = [0] * len(categories)


index = categories.index("electronics")
data_dict["A1000"][index] += 1

data_dict["A2000"] = [0] * len(categories)
data_dict["A3000"] = [0] * len(categories)
data_dict["A4000"] = [0] * len(categories)
s = ","

with open("tes_results.csv", "w") as outfile:
        writer = csv.writer(outfile, delimiter=",")
        for key, value in data_dict.items():
            data = [key] + value
            writer.writerow(data)

print(data_dict)

