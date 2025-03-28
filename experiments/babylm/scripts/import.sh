#!/bin/bash

BABYLM_PATH="/path/to/babylm"  # update on your local machine
INDICES_PATH="experiments/babylm/indices"
LOG_PATH="experiments/babylm/logs"


echo "================================================"
echo "Creating DECAF Indices for Parsed BabyLM Corpora"
echo "================================================"

# import all parsed sub-corpora of BabyLM
mkdir "${INDICES_PATH}"
for subset_path in "${BABYLM_PATH}"/*.complete; do
  subset_file=$(basename "${subset_path}")
  index_path="${INDICES_PATH}/${subset_file%.*}"
  echo "Indexing '${subset_path}' -> '${index_path}'..."

  python3 scripts/import/ud.py \
    --input "${subset_path}" \
    --output "${index_path}" \
    --literal-level "token" | tee -a "${LOG_PATH}/${subset_file%.*}.log"
done