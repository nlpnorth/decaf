#!/bin/bash

INDICES_PATH="experiments/babylm/indices"
OUTPUT_PATH="experiments/babylm/outputs"

python scripts/analyze/overlap.py \
  --indices ${INDICES_PATH}/* \
  --types \
    "Abbr" "Case" "Definite" "Degree" "ExtPos" "Foreign" "Gender" "Mood" "NumForm" "NumType" "Number" "Person" "Polarity" "Poss" "PronType" "Reflex" "Style" "Tense" "Typo" "VerbForm" "Voice" \
    "deprel" "upos" "xpos" \
  --output "${OUTPUT_PATH}/annotation_overlaps.pkl"
