#!/usr/bin/python3

import argparse
import json
import os

from datasets import load_dataset
from transformers import AutoModelForCausalLM, AutoTokenizer, DataCollatorForLanguageModeling, Trainer, TrainingArguments
from torch.utils.data import DataLoader

#
# helper classes
#

class UnshuffledTrainer(Trainer):
	def get_train_dataloader(self) -> DataLoader:
		train_dataset = self.train_dataset
		data_collator = self.data_collator

		dataloader_params = {
		   "shuffle": False,
		   "batch_size": self._train_batch_size,
		   "collate_fn": data_collator,
		   "num_workers": self.args.dataloader_num_workers,
		   "pin_memory": self.args.dataloader_pin_memory,
		   "persistent_workers": self.args.dataloader_persistent_workers,
		}

		return self.accelerator.prepare(DataLoader(train_dataset, **dataloader_params))


#
# helper functions
#

def parse_arguments():
	arg_parser = argparse.ArgumentParser(description='Language Model Training')

	# data setup
	arg_parser.add_argument(
		'--data-path', required=True,
		help='path to pre-processed data')

	# model setup
	arg_parser.add_argument(
		'--base-model', required=True,
		help='identifier of base model'
	)
	arg_parser.add_argument(
		'--model-revision',
		help='revision of base model'
	)

	# training setup
	arg_parser.add_argument(
		'--exp-path', required=True,
		help='path to experiment directory')
	arg_parser.add_argument(
		'--learning-rate', type=float, default=0.001,
		help='learning rate')
	arg_parser.add_argument(
		'--warmup', type=float, default=0.01,
		help='warmup ratio')
	arg_parser.add_argument(
		'--macro-batch-size', type=int, default=128,
		help='macro batch size (default: 1024)')
	arg_parser.add_argument(
		'--micro-batch-size', type=int, default=64,
		help='micro batch size (default: 16)')
	arg_parser.add_argument(
		'--seed', type=int, default=42,
		help='random seed (default: 42)')

	return arg_parser.parse_args()


def preprocess(dataset, tokenizer, batch_length):
	# tokenization helper function
	def preprocess_tokenization(examples):
		return tokenizer(examples['text'])

	# blocking helper function
	def preprocess_blocks(examples):
		# Concatenate all texts.
		concatenated_examples = {k: sum(examples[k], []) for k in examples.keys()}
		total_length = len(concatenated_examples[list(examples.keys())[0]])
		# We drop the small remainder, we could add padding if the model supported it instead of this drop, you can
		# customize this part to your needs.
		total_length = (total_length // batch_length) * batch_length
		# Split by chunks of max_len.
		result = {
			k: [t[i: i + batch_length] for i in range(0, total_length, batch_length)]
			for k, t in concatenated_examples.items()
		}
		result["labels"] = result["input_ids"].copy()
		return result

	# apply tokenization
	dataset = dataset.map(preprocess_tokenization, batched=True, num_proc=4, remove_columns=['text'])

	# apply blocking
	dataset = dataset.map(preprocess_blocks, batched=True, batch_size=1000, num_proc=4)

	return dataset


def setup_trainer(model, dataset, tokenizer, collator, args):
	assert (args.macro_batch_size % args.micro_batch_size) == 0, f"[Error] Macro batch size must be divisible by micro batch size: {args.macro_batch_size} % {args.micro_batch_size} ≠ 0."

	trainer_config = TrainingArguments(
	    output_dir=args.exp_path,
	    eval_strategy='no',
	    learning_rate=args.learning_rate,
		lr_scheduler_type='cosine',
		warmup_ratio=args.warmup,
	    weight_decay=0.1,
		per_device_train_batch_size=args.micro_batch_size,
		gradient_accumulation_steps=(args.macro_batch_size // args.micro_batch_size),
		num_train_epochs=1,
		logging_steps=10,
		save_steps=100,
		seed=args.seed
	)
	trainer = UnshuffledTrainer(
		model=model,
		args=trainer_config,
		train_dataset=dataset,
		data_collator=collator,
		processing_class=tokenizer
	)

	return trainer


def main():
	args = parse_arguments()

	# load pre-training data
	dataset = load_dataset('text', data_files={'train': args.data_path})['train']
	print(f"Loaded pre-training data with {len(dataset)} lines.")
	print(dataset[0])

	# load existing tokenizer
	tokenizer = AutoTokenizer.from_pretrained(args.base_model, revision=args.model_revision, use_fast=True)
	tokenizer.pad_token = tokenizer.eos_token
	print(f"Loaded pre-trained tokenizer from '{args.base_model}' (revision={args.model_revision}).")

	# load existing model
	model = AutoModelForCausalLM.from_pretrained(args.base_model, revision=args.model_revision)
	print(f"Loaded pre-trained model from '{args.base_model}' (revision={args.model_revision}):\n{model}")

	# pre-process dataset
	# dataset = preprocess(dataset, tokenizer, batch_length=model.config.max_position_embeddings)
	dataset = preprocess(dataset, tokenizer, batch_length=256)
	# create data collator (to shift labels to the right)
	collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False)

	# set up trainer
	trainer = setup_trainer(model, dataset, tokenizer, collator, args)

	# main training loop
	trainer.train()

	# save statistics
	statistics_path = os.path.join(args.exp_path, 'training-statistics.json')
	with open(statistics_path, 'w') as fp:
		json.dump(trainer.state.log_history, fp, indent=4)
	print(f"Saved {len(trainer.state.log_history)} training statistic(s) to '{statistics_path}'.")



if __name__ == '__main__':
	main()
