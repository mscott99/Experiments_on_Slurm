#!/bin/bash

# This script should be placed in /scratch or a subfolder, and run from there.

USER="mscott99"
EMAIL="matthewscott@math.ubc.ca"  # Email to send notification to
VENV_ACTIVATE_PATH="/home/mscott99/projects/def-oyilmaz/mscott99/Model-adapted-Fourier-sampling-for-generative-compressed-sensing/cc_venv/bin/activate"
PROJECT_DIR="/home/mscott99/projects/def-oyilmaz/mscott99/Model-adapted-Fourier-sampling-for-generative-compressed-sensing"

ACCOUNT="def-oyilmaz"
# ACCOUNT="rrg-geof"

JOB_NAME="test2"
TIME="00:05:00"     # Max export SBATCH_ACCOUNTexpected time for each job
MEMORY="8GB"       # Max expected memory for each job
CPU_NUM="1"
ROWS_PER_WORKER=3

# The first argument is the path of the python sweep file to run
# This file must handle the two following arguments:
# --get-num-exp : prints the total number of experiments and exits
#                 You can use sweetsweep.get_num_exp()
# --only-exp-id <index>: only run the experiment with this index.
#                        You can use the parameter `only_exp_id` in sweetsweep.parameter_sweep()
# -o, --outdir <dir>: Path to output directory for the entire sweep.
#                     You can pass this to the `sweep_dir` argument of sweetsweep.parameter_sweep()

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

# Make main output directory
OUT_DIR="/home/$USER/scratch/sweep_out/sweep_$JOB_NAME"
mkdir -p $OUT_DIR

END_IND=$(python "$SWEEP_FILE" --get-num-workers -f "$EXP_FILE" --rows-per-worker "$ROWS_PER_WORKER" 2> $OUT_DIR/err_get_ind)

echo "$END_IND" > $OUT_DIR/num_workers

# Make log output directory

job_id=$(sbatch << HEREDOC
#!/bin/bash
#SBATCH --time=0:01:00
#SBATCH --array=1-"$END_IND"
echo "Running job "$SLURM_ARRAY_TASK_ID"" > out.txt

module load StdEnv
module load python
module load scipy-stack

source "$VENV_ACTIVATE_PATH"

mkdir -p $OUT_DIR/exp_\$SLURM_ARRAY_TASK_ID

python "$SWEEP_FILE" --rows-per-worker $ROWS_PER_WORKER -f "$EXP_FILE" -o $OUT_DIR --only-exp-id \$SLURM_ARRAY_TASK_ID -p "$PROJECT_DIR"  > $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/stdout_\${SLURM_ARRAY_TASK_ID} 2> $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/err_\$SLURM_ARRAY_TASK_ID 
HEREDOC
)

# Submit the array job
# job_id=$(sbatch << HEREDOC
# #!/bin/bash
# # This heredocument is the script sent to sbatch
#
# # BE CAREFUL WITH WHEN VARIABLES ARE EVALUATED:
# # - variables like $VAR are evaluated when running this script (the one outside the HEREDOC), so at *submission* time.
# # - variables like \$VAR are evaluated when running the HEREDOC script, so at *execution* time.
#
# #SBATCH --time=$TIME     # Time per job in the array
# #SBATCH --mem=$MEMORY
# #SBATCH --job-name=$JOB_NAME
# #SBATCH --mail-user=$EMAIL
# #SBATCH --mail-type=ALL
# #SBATCH --array=0-"$END_IND"
# #SBATCH --cpus-per-task=$CPU_NUM
#
# module load StdEnv
# module load python
# module load scipy-stack
#
# source "$VENV_ACTIVATE_PATH"
#
# mkdir -p $OUT_DIR/exp_\$SLURM_ARRAY_TASK_ID
#
# python "$SWEEP_FILE" --rows_per_workers $ROWS_PER_WORKER -f "$EXP_FILE" -o $OUT_DIR --only-exp-id \$SLURM_ARRAY_TASK_ID -p "$PROJECT_DIR"  > $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/stdout_\${SLURM_ARRAY_TASK_ID} 2> $OUT_DIR/exp_\${SLURM_ARRAY_TASK_ID}/err_\$SLURM_ARRAY_TASK_ID 
# HEREDOC
# )

job_id=${job_id##* }

echo "Submitted job with ID ${job_id}"

(
sleep 5
while sacct -j "$job_id" -n -o state | grep -qE 'PENDING|RUNNING'; do
    echo "Sleeping..."
    sleep 5
done

python $SWEEP_FILE --cleanup -f "$EXP_FILE" -o $OUT_DIR > $OUT_DIR/cleanup_stdout
) & disown
