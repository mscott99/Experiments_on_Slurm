import pytest
from parallelize_on_slurm import concatenate_experiments
import pandas as pd
import numpy as np

def test_concatenate_experiments():
    # Create a sample original DataFrame
    original_df = pd.DataFrame({
        'param': ['a', 'b', 'c', 'd'],
        'value': [1, 2, 3, 4]
    }, index=[0, 1, 2, 3])
    original_df.attrs = {'test_attr': 'original'}

    # Test Case 1: Empty experiment list
    result1 = concatenate_experiments(original_df, [])
    
    print(result1)
    assert len(result1) == len(original_df)
    assert all(result1['experiment_found'] == False)
    assert result1.attrs == original_df.attrs

    # Test Case 2: Partial matches
    exp_df1 = pd.DataFrame({
        'param': ['a', 'b'],
        'value': [1, 2],
        'result': [10, 20]
    }, index=[0, 1])
    
    result2 = concatenate_experiments(original_df, [exp_df1])
    print(result2)
    assert len(result2) == len(original_df)
    assert result2.loc[0, 'experiment_found'] == True
    assert result2.loc[1, 'experiment_found'] == True
    assert result2.loc[2, 'experiment_found'] == False
    assert result2.loc[3, 'experiment_found'] == False
    assert result2.attrs == original_df.attrs

    # Test Case 3: Multiple experiment DataFrames
    exp_df2 = pd.DataFrame({
        'param': ['c'],
        'value': [3],
        'result': [30]
    }, index=[2])
    
    result3 = concatenate_experiments(original_df, [exp_df1, exp_df2])
    assert len(result3) == len(original_df)
    assert result3.loc[0, 'experiment_found'] == True
    assert result3.loc[1, 'experiment_found'] == True
    assert result3.loc[2, 'experiment_found'] == True
    assert result3.loc[3, 'experiment_found'] == False
    assert result3.attrs == original_df.attrs

    # Test Case 4: Verify data integrity
    assert result3.loc[0, 'result'] == 10
    assert result3.loc[1, 'result'] == 20
    assert result3.loc[2, 'result'] == 30
    assert pd.isna(result3.loc[3, 'result'])

    # Test Case 5: Verify index ordering
    assert list(result3.index) == [0, 1, 2, 3]
