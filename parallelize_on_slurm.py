import argparse
from typing import Callable, Any, Union
import glob
import os
from re import error
import pandas as pd
import sys
import importlib.util
import time
from pathlib import Path

SEED=1234

def load_module(module_path):
    module_name = Path(module_path).stem
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if (spec is None):
        error("Experiment file not found")
        exit(1)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    if (spec.loader is None):
        error("No loader")
        exit(1)
    spec.loader.exec_module(module)
    return module

def cleanup(original_df:pd.DataFrame, output_dir, max_retries=3, retry_delay=5):
    pickle_glob = os.path.join(output_dir, "exp_*/processed_*.pkl")
    experiment_dfs = []
    file_obj = None
    for file in glob.glob(pickle_glob):
        for attempt in range(max_retries):
            try:
                file_obj = pd.read_pickle(file)
            except (EOFError, BrokenPipeError) as e:
                if attempt == max_retries - 1:
                    raise
                print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        if (file_obj is None):
            sys.stderr.write("Reading file yields None: " + file)
        experiment_dfs.append(file_obj)
    all_experiments_df = pd.concat(experiment_dfs, ignore_index=False).sort_index()
    all_experiments_df.attrs = original_df.attrs
    if len(all_experiments_df) != len(original_df):
        sys.stderr.write("Could not find all experiment results. See the missing rows in MISSING_ROWS.pickle")
        missing_idx = original_df.index.difference(all_experiments_df.index)
        original_df.loc[missing_idx].to_pickle(os.path.join(
        output_dir, "MISSING_ROWS.pickle"))
    all_experiments_df.to_pickle(os.path.join(
        output_dir, "combined_results.pickle"))


def main(get_num_workers: bool, do_cleanup: bool, rows_per_worker: int, exp_file: str, output_dir: str, exp_id: int, project_dir: str, seed=SEED):
    sys.path.append(project_dir)
    module = load_module(exp_file)
    make_df:Callable[[], pd.DataFrame] = getattr(module, 'make_df')
    experiment:Callable[[Union[dict, pd.Series]], dict[str, Any]] = getattr(module, 'experiment')
    if (get_num_workers):
        print(-(-len(make_df())//rows_per_worker)) # negative signs for good rounding
        return
    df = make_df().sample(frac=1, random_state=seed)
    if (do_cleanup):
        cleanup(df, output_dir)
        return
    output_subfolder = os.path.join(output_dir, "exp_" + str(exp_id))
    os.makedirs(output_subfolder, exist_ok=True)
    total_rows = len(df)
    start_idx = (exp_id-1) * rows_per_worker
    end_idx = min(exp_id * rows_per_worker, total_rows)
    chunk_df = df.iloc[start_idx:end_idx].copy()
    output = chunk_df.apply(lambda row: experiment(
        {**chunk_df.attrs, **row.to_dict()}), 1, result_type='expand') # prioritizes rows over attrs.
    processed_df = pd.concat([chunk_df, output], axis=1)
    output_file = os.path.join(output_subfolder, f"processed_{exp_id}.pkl")
    processed_df.to_pickle(output_file)
    print(f"Processed rows {start_idx} to {end_idx} saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine the results of a dataframe run.")
    parser.add_argument("--get-num-workers",
                        action='store_true', required=False, help="number of rows each worker should process.")
    parser.add_argument("--cleanup", action='store_true', required=False,
                        help="Combine the results of a dataframe run.")
    parser.add_argument("-f", "--exp-file", required=False,
                        help="Experiment file.")
    parser.add_argument("--rows-per-worker", required=False, type=int,
                        help="Experiment file.")
    parser.add_argument("--only-exp-id", required=False, type=int,
                        help="ID of the experiment.")
    parser.add_argument("-p", "--project-dir", required=False,
                        help="Path to project dir with possible dependent scripts to import with python.")
    parser.add_argument("-o", "--output-dir", required=False,
                        help="Output directory")
    args = parser.parse_args()
    main(args.get_num_workers, args.cleanup, args.rows_per_worker, args.exp_file,
         args.output_dir, args.only_exp_id, args.project_dir)
