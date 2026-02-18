#!/usr/bin/env bash
#SBATCH --job-name=merge_month
#SBATCH --account=s2441
#SBATCH --time=0:40:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=file.%j.out
#SBATCH --error=file.%j.err

set -euo pipefail
module load python/GEOSpyD

YEAR=2024
MONTH=6
MODEL="GEOSFP"

python -u merge_digests_and_write_pickle.py \
  --model "$MODEL" \
  --year "$YEAR" \
  --month "$MONTH"


