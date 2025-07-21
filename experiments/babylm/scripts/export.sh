#!/bin/bash

INDICES_PATH="experiments/babylm/indices"
OUTPUT_PATH="experiments/babylm/outputs/exports/all"

for index in "${INDICES_PATH}"/*; do
  python scripts/export/all.py \
    --index "${index}" \
    --structure "sentence" \
    --output "${OUTPUT_PATH}/$(basename "${index}").txt"
done

cat "${OUTPUT_PATH}"/*.txt > "${OUTPUT_PATH}/babylm.txt"