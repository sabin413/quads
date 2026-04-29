#!/usr/bin/env bash
#SBATCH --job-name=quads_user
#SBATCH --account=s2441
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --array=1-31
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/user.%A_%a.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/user.%A_%a.err

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev

module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

# -----------------------------
# User inputs (edit these only)
# -----------------------------
MODEL="GEOSIT" # should work for both types
DATE="2024-02"

DAY=$(printf "%02d" "${SLURM_ARRAY_TASK_ID}")

days_in_month=$(python - <<PY
from calendar import monthrange
y, m = map(int, "${DATE}".split("-"))
print(monthrange(y, m)[1])
PY
)

if (( 10#$DAY > days_in_month )); then
    echo "Skipping invalid date ${DATE}-${DAY}"
    exit 0
fi

DATE="${DATE}-${DAY}"

# Export to Python
export MODEL DATE

echo "Submitting QUADS user job"
echo "MODEL=$MODEL"
echo "DATE=$DATE"

python -u -m quads.for_users
