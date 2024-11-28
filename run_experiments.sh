#!/bin/bash
# This script should be placed in /scratch or a subfolder, and run from there.

USER="mscott99"
EMAIL="matthewscott@math.ubc.ca"  # Email to send notification to
VENV_ACTIVATE_PATH="/home/mscott99/projects/def-oyilmaz/mscott99/Sparse_adapted_denoising/venv/bin/activate"
PROJECT_DIR="/home/mscott99/projects/def-oyilmaz/mscott99/Sparse_adapted_denoising"
ACCOUNT="def-oyilmaz"
JOB_NAME="larger"
TIME="02:00:00"    # Max export SBATCH_ACCOUNTexpected time for each job
MEMORY="1G"      # Max expected memory for each job
CPU_NUM="1"
ROWS_PER_WORKER=50 # 50 ends in 30 ish minutes

# ARGUMENTS
# The first argument is the path of the python sweep file to run
# The second argument is the experiment file, which must specify the "make_df" function and the "experiment function"
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <SWEEP_FILE> <EXPERIMENT_FILE>"
    exit 1
fi
SWEEP_FILE="$1"
EXP_FILE="$2"

module load StdEnv
module load python
module load scipy-stack
source "$VENV_ACTIVATE_PATH"

BASE_OUT_DIR="/home/$USER/scratch/sweep_out/$JOB_NAME"
create_unique_dir() {
    local base_path="$1"
    local dir_path="$base_path"
    local counter=1
    
    # If directory exists, append increasing numbers until we find a free name
    while [[ -d "$dir_path" ]]; do
        dir_path="${base_path}_${counter}"
        ((counter++))
    done
    
    # Create the directory
    mkdir -p "$dir_path"
    echo "$dir_path"
}
OUT_DIR=$(create_unique_dir "$BASE_OUT_DIR")
mkdir -p "$OUT_DIR"
END_IND=$(python "$SWEEP_FILE" --get-num-workers -f "$EXP_FILE" --rows-per-worker "$ROWS_PER_WORKER")
echo "$END_IND" > "$OUT_DIR"/num_workers

# Make log output directory

# let PYTHONPATH="$PYTHONPATH":"$PROJECT_DIR"
job_id=$(sbatch << HEREDOC
#!/bin/bash
#SBATCH --mail-user="$EMAIL"
#SBATCH --mail-type=END
#SBATCH --time="$TIME"
#SBATCH --array=1-"$END_IND"
#SBATCH --cpus-per-task="$CPU_NUM"
#SBATCH --output="$OUT_DIR"/Logs/bash_task_out_%A_%a.out
#SBATCH --account="$ACCOUNT"
#SBATCH --mem="$MEMORY"

# BE CAREFUL WITH WHEN VARIABLES ARE EVALUATED:
# - variables like $VAR are evaluated when running this script (the one outside the HEREDOC), so at *submission* time.
# - variables like \$VAR are evaluated when running the HEREDOC script, so at *execution* time.

module load StdEnv
module load python
module load scipy-stack

source "$VENV_ACTIVATE_PATH"
mkdir -p $OUT_DIR/exp_\$SLURM_ARRAY_TASK_ID
python "$SWEEP_FILE" --rows-per-worker $ROWS_PER_WORKER -f "$EXP_FILE" -o $OUT_DIR --only-exp-id \$SLURM_ARRAY_TASK_ID -p "$PROJECT_DIR"  > $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/stdout_\${SLURM_ARRAY_TASK_ID} 2> $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/err_\$SLURM_ARRAY_TASK_ID 
HEREDOC
)

job_id=${job_id##* }
echo "Submitted job with ID ${job_id}"
echo "$job_id" > $OUT_DIR/JOB_ID
(
sleep 30 # Give enough time for the system to register the job before checking
while sacct -j "$job_id" -n -o state | grep -qE 'PENDING|RUNNING'; do
    sleep 10
done
sleep 30 # Wait for filesystem sync
python "$SWEEP_FILE" --cleanup -f "$EXP_FILE" -o "$OUT_DIR" > "$OUT_DIR"/cleanup_stdout.log 2> "$OUT_DIR"/cleanup_ERR.log
echo "Job $job_id completed at $(date)" > "$OUT_DIR/completed.log"
) & disown
echo "Submission completed"
