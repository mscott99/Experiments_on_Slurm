import pandas as pd

def make_df():
    return pd.DataFrame({'A': [1,2,3,4,5,6], 'B': [6,5,4,3,2,1]})

def experiment(data):
    return {'C': data['A'] + data['B']}
