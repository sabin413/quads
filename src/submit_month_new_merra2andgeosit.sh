#!/usr/bin/env bash
#SBATCH --job-name=quads_merra2
#SBATCH --account=s2441
#SBATCH --constraint=mil
#SBATCH --qos=long
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=file.%A_%a.out     # %A = job ID, %a = array index
#SBATCH --error=file.%A_%a.err            
module load python/GEOSpyD

# Hard-coded year, month, model for this run
YEAR=2024
MONTH=5
DAY=1 # day is dummy here 
MODEL="MERRA2"	#

# zero-pad month and day
MONTH_PADDED=$(printf "%02d" "${MONTH}")
DAY_PADDED=$(printf "%02d" "${DAY}")

# build a candidate date string
DATE_STR="${YEAR}-${MONTH_PADDED}-${DAY_PADDED}"

echo "Running for DATE=${DATE_STR}, MODEL=${MODEL}"

srun python -u compute_and_save_daily_digests.py \
    --date "${DATE_STR}" \
    --model "${MODEL}"

