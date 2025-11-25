#!/usr/bin/env bash
#SBATCH --job-name=data_compress          # job name shown by squeue
#SBATCH --account=s2441        # replace with your NCCS project code
#SBATCH --time=03:00:00             # wall-clock limit (HH:MM:SS)
#SBATCH --output=file.%j.out   # stdout goes here  (%j = job-ID)
#SBATCH --error=file.%j.err    # stderr goes herei

module load python/GEOSpyD
##python lowm_para_serial.py
##python run_monthly_batch.py
#python compute_and_save_daily_digests.py
#python merge_digests_and_write_pickle.py
python lowm_para_serial_for_users.py
