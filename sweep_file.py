import argparse
import glob
import os
from re import error
import pandas as pd
import sys
import importlib.util
from pathlib import Path


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


def cleanup(original_df, output_dir):
    pickle_glob = os.path.join(output_dir, "exp_*/processed_*.pkl")

    # Collect all experiment dataframes
    experiment_dfs = []
    for file in glob.glob(pickle_glob):
        file_obj = pd.read_pickle(file)
        if(file_obj is None):
            raise Exception("Reading file yields None: " + file)
        experiment_dfs.append(file_obj)

    # Concatenate all experiment dataframes
    all_experiments_df = pd.concat(experiment_dfs, ignore_index=False)

    # Merge with the original dataframe
    all_experiments_df.attrs = original_df.attrs

    # Save the result
    print(all_experiments_df)
    all_experiments_df.to_pickle(os.path.join(
        output_dir, "combined_results.pickle"))


def main(get_num_workers: bool, do_cleanup: bool, rows_per_worker: int, exp_file: str, output_dir: str, exp_id: int, project_dir: str):
    sys.path.append(project_dir)
    module = load_module(exp_file)
    make_df = getattr(module, 'make_df')
    experiment = getattr(module, 'experiment')
    # Ensure the output subfolder exists
    if (get_num_workers):
        if(rows_per_worker == None):
            raise Exception("rows_per_worker is required to get the number of rows.")
        print(-(-make_df().shape[0]//rows_per_worker))
        return

    df = make_df()
    if (do_cleanup):
        cleanup(df, output_dir)
        return
    if(output_dir == None):
        raise Exception("Output directory is required.")
    if(exp_id == None):
        raise Exception("exp_id is required.")
    if(rows_per_worker == None):
        raise Exception("rows_per_worker is required.")
    output_subfolder = os.path.join(output_dir, "exp_" + str(exp_id))
    os.makedirs(output_subfolder, exist_ok=True)
    # Load the DataFrame
    total_rows = len(df)

    # Calculate the start and end indices for this task
    start_idx = (exp_id-1) * rows_per_worker
    end_idx = min(exp_id * rows_per_worker, total_rows)

    # Process the chunk
    chunk_df = df.iloc[start_idx:end_idx].copy()
    output = chunk_df.apply(lambda row: experiment(
        {**row.to_dict(), **chunk_df.attrs}), 1, result_type='expand')
    processed_df = pd.concat([chunk_df, output], axis=1)
    print(processed_df)
    # Save the processed chunk
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
    # parser.add_argument(
    # "-f", "--df_file", help="Path to the pickle file containing the setup DataFrame")
    parser.add_argument("-o", "--output-dir", required=False,
                        help="Output directory")
    args = parser.parse_args()
    main(args.get_num_workers, args.cleanup, args.rows_per_worker, args.exp_file,
         args.output_dir, args.only_exp_id, args.project_dir)
