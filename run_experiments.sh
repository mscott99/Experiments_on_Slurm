#!/bin/bash
# This script should be placed in /scratch or a subfolder, and run from there.

USER="mscott99"
EMAIL="matthewscott@math.ubc.ca"  # Email to send notification to
ACCOUNT="def-oyilmaz"
JOB_NAME="Mai_2025"
TIME="03:00:00"    # Max export SBATCH_ACCOUNTexpected time for each job
MEMORY="2G" 
CPU_NUM="1"
GPU_NUM="0"
ROWS_PER_WORKER=200 # 40 for sparse, 20 for gen MNIST.

# ARGUMENTS
# The first argument is the path of the python sweep file to run
# The second argument is the experiment file, which must specify the "make_df" function and the "experiment function"

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 <EXPERIMENT_MODULE> <OUT_DIR> [<PATH_TO_parallelize_on_slurm>]"
    exit 1
fi

SWEEP_FILE="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"/parallelize_on_slurm.py
if [ -f "$SWEEP_FILE" ]; then
    SWEEP_FILE="$SWEEP_FILE"
elif [ "$#" -eq 3 ]; then
    SWEEP_FILE="$3"
else
    echo "Error: Sweep file not found at default location and no alternative path provided."
    exit 1
fi

EXPERIMENT_MODULE="$1"

if [ -z "$VENV" ]; then
    echo "Env var VENV not set, guessing venv in project."
    VENV="$EXPERIMENT_MODULE/.venv/bin/activate"
fi

PROJECT_HEAD=""
if git -C "$EXPERIMENT_MODULE" rev-parse --git-dir > /dev/null 2>&1; then
    PROJECT_HEAD=$(git -C "$EXPERIMENT_MODULE" rev-parse HEAD)
fi

BASE_OUT_DIR="$2"

module load StdEnv
module load python
module load scipy-stack

source "$VENV"

JOB_OUT_DIR="$BASE_OUT_DIR"/"$JOB_NAME"
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
OUT_DIR=$(create_unique_dir "$JOB_OUT_DIR")
mkdir -p "$OUT_DIR"
END_IND=$(python "$SWEEP_FILE" --get-num-workers -f "$EXPERIMENT_MODULE" --rows-per-worker "$ROWS_PER_WORKER")
echo "$END_IND" > "$OUT_DIR"/num_workers.log

if ! [[ "$END_IND" =~ ^[0-9]+$ ]]; then
    echo "Error: END_IND is not a valid integer. Value received: $END_IND"
    exit 1
fi

# Make log output directory

# let PYTHONPATH="$PYTHONPATH":"$PROJECT_DIR"
job_id=$(sbatch << HEREDOC
#!/bin/bash
#SBATCH --mail-user="$EMAIL"
#SBATCH --mail-type=END
#SBATCH --time="$TIME"
#SBATCH --array=1-"$END_IND"
#SBATCH --cpus-per-task="$CPU_NUM"
#SBATCH --ntasks=1
$( [ "$GPU_NUM" -gt 0 ] && echo "#SBATCH --gpus-per-node=$GPU_NUM" )
#SBATCH --output="$OUT_DIR"/Logs/bash_task_out_%A_%a.out
#SBATCH --account="$ACCOUNT"
#SBATCH --mem="$MEMORY"

# BE CAREFUL WITH WHEN VARIABLES ARE EVALUATED:
# - variables like $VAR are evaluated when running this script (the one outside the HEREDOC), so at *submission* time.
# - variables like \$VAR are evaluated when running the HEREDOC script, so at *execution* time.

module load StdEnv
module load python
module load scipy-stack

$([ -n "$VENV" ] && echo "source $VENV")

mkdir -p $OUT_DIR/exp_\$SLURM_ARRAY_TASK_ID
python "$SWEEP_FILE" --rows-per-worker $ROWS_PER_WORKER -f "$EXPERIMENT_MODULE" -o $OUT_DIR --only-exp-id \$SLURM_ARRAY_TASK_ID > $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/stdout_\${SLURM_ARRAY_TASK_ID} 2> $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/err_\$SLURM_ARRAY_TASK_ID 
HEREDOC
)

job_id=${job_id##* }
echo "$job_id" > "$OUT_DIR"/JOB_ID
(
sleep 30 # Give enough time for the system to register the job before checking
while sacct -j "$job_id" -n -o state | grep -qE 'PENDING|RUNNING'; do
    sleep 10
done
sleep 30 # Wait for filesystem sync
python "$SWEEP_FILE" --cleanup -f "$EXPERIMENT_MODULE" -o "$OUT_DIR"  -c "$PROJECT_HEAD" > "$OUT_DIR"/cleanup_stdout.log 2> "$OUT_DIR"/cleanup_ERR.log
echo "Job $job_id completed at $(date)" > "$OUT_DIR/completed.log"
if [ -f "$BASE_OUT_DIR"/../running.lock ]; then
    rm "$BASE_OUT_DIR"/../running.lock
fi
) & disown
echo "Submitted job with ID ${job_id}"
