#!/usr/bin/env bash
#SBATCH --job-name=quads_type2
#SBATCH --account=s2441
#SBATCH --constraint=mil
#SBATCH --qos=long
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/typeB/file.%j.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/typeB/file.%j.err

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev

module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate

# Year/month/model for this run (allow sbatch --export to override)
YEAR=${YEAR:-2024}
MONTH=${MONTH:-5}
MODEL=${MODEL:-MERRA2}

DAY=1  # dummy day - can use any value for the day, but an input is needed for formatting consistency

MONTH_PADDED=$(printf "%02d" "${MONTH}")
DAY_PADDED=$(printf "%02d" "${DAY}")
DATE_STR="${YEAR}-${MONTH_PADDED}-${DAY_PADDED}"

echo "Running for DATE=${DATE_STR}, MODEL=${MODEL}"

srun python -u -m quads.compute_and_save_daily_digests \
    --date "${DATE_STR}" \
    --model "${MODEL}"

