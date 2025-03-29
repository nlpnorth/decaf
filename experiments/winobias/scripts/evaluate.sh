#!/bin/bash

MODEL_PATH='models'
OUTPUT_PATH='evaluations'

SEEDS=( 0 1 2 3 4 )
SETUPS=( 'fm' 'mf' )

for seed in "${SEEDS[@]}"; do
  for setup in "${SETUPS[@]}"; do
    for checkpoint in "${MODEL_PATH}/pythia-14m-s${seed}-${setup}/checkpoint-"*; do
      step="${checkpoint##*-}"
      results_path="${OUTPUT_PATH}/winobias-pythia-14m-s${seed}-${setup}-step${step}.json"
      if [ -f "${results_path}" ]; then
        continue
      fi
      python scripts/winobias.py --model "${checkpoint}" --output "${results_path}"
    done
  done
done