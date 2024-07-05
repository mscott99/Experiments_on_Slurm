import itertools
import pandas as pd

def make_df():
    return pd.DataFrame(itertools.product(range(2), range(2)), columns=['a', 'b'])

def experiment(row):
    return {'result': row['a'] + row['b']}
