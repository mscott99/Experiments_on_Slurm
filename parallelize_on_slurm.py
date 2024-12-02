import traceback
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

SEED = 1234


def load_module(module_path):
    print("loading module")
    path = Path(module_path)
    if path.is_dir():
        package_name = path.name
        init_file = path / "__init__.py"
        if not init_file.exists():
            raise ImportError(
                f"Directory {module_path} is not a valid Python package (missing __init__.py)")
        spec = importlib.util.spec_from_file_location(
            package_name, str(init_file))
        if spec is None:
            raise ImportError(f"Could not load package from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[package_name] = module
        if spec.loader is None:
            raise ImportError(f"No loader for package {package_name}")
        spec.loader.exec_module(module)
    else:
        module_name = path.stem
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None:
            raise ImportError(f"Module file {module_path} not found")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        if spec.loader is None:
            raise ImportError(f"No loader for module {module_name}")
        spec.loader.exec_module(module)
    return module


def cleanup(original_df: pd.DataFrame, output_dir, max_retries=3, retry_delay=5):
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
                print(
                    f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        if (file_obj is None):
            sys.stderr.write("Reading file yields None: " + file)
        experiment_dfs.append(file_obj)
    all_experiments_df = pd.concat(
        experiment_dfs, ignore_index=False).sort_index()
    all_experiments_df.attrs = original_df.attrs
    all_experiments_df.to_pickle(os.path.join(
        output_dir, "combined_results.pickle"))


def main(get_num_workers: bool, do_cleanup: bool, rows_per_worker: int, exp_module_path: str, output_dir: str, exp_id: int, project_dir: str, seed=SEED):
    module = load_module(exp_module_path)
    make_df: Callable[[], pd.DataFrame] = getattr(module, 'make_df')
    experiment: Callable[[Union[dict, pd.Series]],
                         dict[str, Any]] = getattr(module, 'experiment')
    if (get_num_workers):
        # negative signs for good rounding
        print(-(-len(make_df())//rows_per_worker))
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

    def safe_exp(data):
        try:
            return experiment(data)
        except Exception as e:
            # has the effect of logging errors into the resulting DataFrame
            return {"ERROR": traceback.format_exc()}
    output = chunk_df.apply(lambda row: safe_exp(
        # prioritizes rows over attrs.
        {**chunk_df.attrs, **row.to_dict()}), 1, result_type='expand')
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
