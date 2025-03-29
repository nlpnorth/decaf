#!/bin/bash

DATA_PATH='../babylm/outputs/winobias'
MODEL_PATH='models'

MODEL='EleutherAI/pythia-14m'
SEEDS=( 0 1 2 3 4 )
SETUPS=( 'f-m' 'm-f' )

for seed in "${SEEDS[@]}"; do
  for setup in "${SETUPS[@]}"; do
    base_model="${MODEL}"
    if [ "$seed" -gt 0 ]; then
      base_model="${base_model}-seed${seed}"
    fi
    python scripts/train/causal.py \
      --data-path "${DATA_PATH}/corpus-${setup}.txt" \
      --base-model "${base_model}" \
      --model-revision "step0" \
      --exp-path "${MODEL_PATH}/pythia-14m-s${seed}-${setup//-}"
  done
done