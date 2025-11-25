#!/usr/bin/env bash
#SBATCH --job-name=data_compress
#SBATCH --account=s2441
#SBATCH --time=03:00:00
#SBATCH --nodes=1
#SBATCH --output=file.%A_%a.out     # %A = job ID, %a = array index
#SBATCH --error=file.%A_%a.err
#SBATCH --array=1-31                # max days; extra days will exit fast

module load python/GEOSpyD

# Hard-coded year, month, model for this run
YEAR=2024
MONTH=5
MODEL="GEOSCF"	#"GEOSFP"

# zero-pad month and day
MONTH_PADDED=$(printf "%02d" "${MONTH}")
DAY_PADDED=$(printf "%02d" "${SLURM_ARRAY_TASK_ID}")

# build a candidate date string
DATE_STR="${YEAR}-${MONTH_PADDED}-${DAY_PADDED}"

# Validate the date and ensure it is still in the same month.
CHECK_MONTH=$(date -d "${DATE_STR}" +%m 2>/dev/null || echo "XX")
if [ "${CHECK_MONTH}" != "${MONTH_PADDED}" ]; then
    echo "Skipping invalid date: ${DATE_STR}"
    exit 0
fi

echo "Running for DATE=${DATE_STR}, MODEL=${MODEL}"

python compute_and_save_daily_digests.py \
    --date "${DATE_STR}" \
    --model "${MODEL}"

