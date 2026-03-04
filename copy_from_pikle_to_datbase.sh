#!/usr/bin/env bash
#SBATCH --job-name=copy_month_to_sqlite
#SBATCH --account=s2441
#SBATCH --time=0:20:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.err

set -euo pipefail
module load python/GEOSpyD

# Year/month/model for this run (allow sbatch --export to override)
YEAR=${YEAR:-2024}
MONTH=${MONTH:-4}
MODEL=${MODEL:-GEOSFP}

echo "Copying monthly digests to SQLite for MODEL=${MODEL}, YEAR=${YEAR}, MONTH=${MONTH}"

python -u -m quads.copy_from_monthly_pickle_to_sqlitedb

