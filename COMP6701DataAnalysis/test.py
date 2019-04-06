import pandas as pd
from random import randint
import numpy as np

df = pd.DataFrame({'A': pd.Series([1, 2, 3, 4, 5]), 'B': pd.Series([1, 2, 3, 4, 5])})

print(df)

num_categories = len(df.columns)
#num_users = len(df.rows)

my_shape = df.shape
print(my_shape)
num_categories = my_shape[1]
num_users = my_shape[0]

df.loc[num_users] = [0 for i in range(num_categories)]

print(df)

df2 = pd.DataFrame()

num_categories2 = df2.shape[1]
num_users = df2.shape[0]
if not 'user1' in df2.index:
    if not 'col1' in df2.columns:
        df2.loc['user1', 'col1'] = 5

if not 'user2' in df2.index:
    if not 'col2' in df2.columns:
        df2.loc['user2', 'col2'] = 6

df2.fillna(0, inplace=True)

df2.loc['user3', 'col3'] = 8

print(df2[df2.columns[2:]])

print(df2)

df3 = pd.DataFrame(columns=['helpfule', 'score'])

df3 = df3.append({'helpfule': 1, 'score': 2}, ignore_index=True)
df3 = df3.append({'helpfule': 1, 'score': 2}, ignore_index=True)
df3 = df3.append({'helpfule': 1, 'score': 2}, ignore_index=True)

print(df3)


testArray = np.array([[1, 2, 3], [4, 5, 5]])

testArray = np.append(testArray, [[7, 8, 9]], axis=0)
print(testArray)
np.savetxt('testResult.csv', testArray, delimiter=",")