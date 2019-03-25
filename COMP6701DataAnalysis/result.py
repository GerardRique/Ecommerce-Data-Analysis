import random
import itertools as it
import heapq


def key_generator():
    charcters = [chr(random.randint(65, 90)) for x in range(100)]
    for i in it.count():
        yield ''.join(random.sample(charcters, 3))

def sort_heapq(my_dict):
    items = [(-value, key) for key, value in my_dict.items()]
    smallest = heapq.nsmallest(1, items)
    return [-value for value, key in smallest]

the_dict = dict((key, random.randint(-500, 500)) for key, _ in zip(key_generator(), range(3000)))
the_dict = sort_heapq(the_dict)
f = open("test.txt", "w")
for key in the_dict:
    f.write(str(key) + "\n")

f.close()

data = [(12, "first"), (24, "second"), (20, "third"), (31, "fourth"),  (20, "fifth"), (19, "sixth")]
result = heapq.nsmallest(3, data)
print(result)

