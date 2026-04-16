#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Inputs
# -----------------------------
YEAR=2023
MONTH=1
MODEL="GEOSCF"

# -----------------------------
# Pipeline submission
# -----------------------------
jid3=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pikle_to_datbase.sh)
echo "Submitted step3 (sqlite populate): $jid3"

echo "Done. Jobs:"
echo "  step3: $jid3"
