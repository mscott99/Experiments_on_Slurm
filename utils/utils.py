import pandas as pd
import itertools
import datetime
import numpy as np
import subprocess
from typing import Callable, Union, Tuple

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

def cross_merge_dfs(df1, df2):
    return pd.merge(df1, df2, how='cross')

def run_local_df_experiment(df:pd.DataFrame, fn:Callable[[dict], pd.Series], new_col_names:Union[list[str], Tuple[str]])-> pd.DataFrame:
    """
    Run the df method for a df that possibly has attributes.
    Args:
        df: Dataframe with the experiment data.
        fn: The row-wise function, should return a pandas series with the value of the new columns.
        new_col_names: names of the new columns
    Returns:
        bool: The extended dataframe.
    """
    return pd.concat([df, df.apply(lambda row: fn({**row.to_dict(), **df.attrs}),1)], axis=1)

def get_commit():
    try:
        commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode('utf-8')
        return commit_hash
    except subprocess.CalledProcessError as e:
        print("Error obtaining current commit hash:", e)
        return None

def add_standard_attributes(df:pd.DataFrame):
    df.attrs = df.attrs | {'seed':1234, 'datetime':datetime.datetime.now().strftime('%Y-%m-%d'), 'commit': get_commit()}
