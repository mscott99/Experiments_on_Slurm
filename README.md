# Run experiments in parallel on slurm
## Usage
1. Draft a script `your_experiment.py` in the same format as `experiment_script.py`. It must implement two functions: `make_df(): pd.DataFrame`, and `experiment(row:pd.Series):Dict`. 
	- `make_df` returns a DataFrame which has each row corresponding to an experiment that should be run. The columns correspond to different hyperparameters specified for a particular experiment. Any elements in the `attrs` of the dataframe will be preserved when running the experiments.
	- `experiment(row):Dict` takes as an argument a single row of the dataframe returned by `make_df()`. It should perform the experiment, and then return a dict with labelled results of the experiment. Each key in the dict will correspond to a new column in the resulting dataframe.
	- The script can import from scripts in a project by specifying the `PROJECT_DIR` path in `run_experiments.sh`.
2. Place all three scripts `experiment_script.py` `parallelize_on_slurm.py` `run_experiments.sh` in a directory like the `scratch` directory.
3. Modify the running parameters within `run_experiments.sh`.
4. In a shell on the sever, navigate to the directory containing the three scripts, and run the command
``` bash
./run_experiments.sh ./parallelize_on_slurm.py ./your_experiment.py 
```
## Results
The script produces an output dataframe which is the dataframe created by `make_df()` with additional rows containing the results of the experiments. This output will be written to the file `$OUTPUT_DIR/$JOB_NAME/combined_results.pickle`, with the bash variables as specified in `run_experiments.sh`. For quick checks of the results, a printout of the resulting dataframe is available in `$OUTPUT_DIR/$JOB_NAME/cleanup_stdout`. Other information about individual tasks will be logged in the same directory.
