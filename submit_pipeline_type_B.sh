#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Inputs (edit these)
# -----------------------------
YEAR=2023
MONTH=3
MODEL="GEOSIT"   # e.g., MERRA2 or GEOSIT

# -----------------------------
# Pipeline submission (Type B: no merge step)
# -----------------------------
jid1=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL submit_month_type_B.sh)
echo "Submitted step1 (monthly/mostly-aggregated digests): $jid1"

jid3=$(sbatch --parsable --dependency=afterok:$jid1 --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pikle_to_datbase.sh)
echo "Submitted step2 (sqlite populate): $jid3"

echo "Done. Jobs:"
echo "  step1: $jid1"
echo "  step2: $jid3"

