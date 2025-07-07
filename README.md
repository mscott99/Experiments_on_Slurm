# Run experiments in parallel on slurm
## Usage
1. Make a python module (which can be a single python script `your_experiment.py` in the same format as `experiment_script.py`). It must export two functions: `make_df(): pd.DataFrame`, and `experiment(row:pd.Series):Dict`. 
	- `make_df` returns a DataFrame which has each row corresponding to an experiment that should be run. The columns correspond to different hyperparameters specified for a particular experiment. Any elements in the `attrs` of the dataframe will be preserved when running the experiments.
	- `experiment(row):Dict` takes as an argument a single row of the dataframe returned by `make_df()`. It should perform the experiment, and then return a dict with labelled results of the experiment. Each key in the dict will correspond to a new column in the resulting dataframe.
2. Clone this repository in a directory like the `scratch` directory on the server.
3. Modify the running parameters within `run_experiments.sh`, including the "BASE_OUT_DIR" variable defined lower.
4. In a shell on the server, from the root of the cloned repository, run the command
``` bash
./run_experiments.sh <PATH_TO_EXPERIMENT_MODULE> <PATH_TO_OUTPUT_DIR> ./parallelize_on_slurm.py 
```
## Results
The script produces an output DataFrame which is the DataFrame created by `make_df()` with additional rows containing the results of the experiments. This output will be written to the file `$OUTPUT_DIR/$JOB_NAME/combined_results.pickle`, with the bash variables as specified in `run_experiments.sh`. For quick checks of the results, a printout of the resulting DataFrame is available in `$OUTPUT_DIR/$JOB_NAME/cleanup_stdout`. Other information about individual tasks will be logged in the same directory.
## Future features
Add a check before the start of the run, that the dataframe has all required keys.
