import pandas as pd
import itertools
import numpy as np

def logspace(start, stop, length, base=2, integer=False):
    if start <= 0 or stop <= 0:
        raise ValueError("start and stop must be positive")
    results = base ** np.linspace(np.log(start)/np.log(base), np.log(stop)/np.log(base), length)
    if integer:
        return np.round(results).astype(int)
    return results

def allcombinations(mydict:dict) -> pd.DataFrame:
    # mydict = a
    for (key, value) in mydict.items():
        if not isinstance(value, (list, tuple, set, pd.Series, np.ndarray)):
            mydict[key] = [value]  # Wrap in a list
    combinations = list(itertools.product(*mydict.values()))
    return pd.DataFrame.from_records(combinations, columns=mydict.keys())

def repeat(df: pd.DataFrame, times ) -> pd.DataFrame:
    return pd.concat([df]*times, ignore_index=True)

def evaluate_by_row(df:pd.DataFrame, new_name:str, fn):
    df[new_name]= df.apply(lambda row: fn(**row), axis=1)
