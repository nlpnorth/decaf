# WinoBias

This directory contains the code for generating targeted training interventions for WinoBias. First, we need some additional packages for model training:

```bash
pip install -r requirements.txt
```

## Generating Training Data

Given the parsed BabyLM index, we construct the training data interventions as follows:

```bash
mkdir ../babylm/outputs/winobias/
python scripts/intervention.py \
  --indices ../babylm/indices/* \
  --output ../babylm/outputs/winobias/
```

## Training Models

We provide a custom model training script, which prevents additional data shuffling. To reproduce the training runs from the paper, run:

```bash
./scripts/train.sh
```

## Evaluating Models

To evaluate all model checkpoints for each seed on WinoBias, run:
```bash
./scripts/evaluate.sh
```