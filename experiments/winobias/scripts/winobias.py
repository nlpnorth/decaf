#!/usr/bin/python3

import argparse, json

import torch

from datasets import load_dataset
from transformers import AutoModelForCausalLM, AutoTokenizer


def parse_arguments():
	arg_parser = argparse.ArgumentParser(description='WinoBias Evaluation')

	arg_parser.add_argument(
		'--model', required=True,
		help='identifier of model'
	)
	arg_parser.add_argument(
		'--model-revision',
		help='revision of base model'
	)
	arg_parser.add_argument(
		'--split', default='validation', choices=['validation', 'test'],
		help='target split of the dataset'
	)
	arg_parser.add_argument(
		'--output', required=True,
		help='path to results file'
	)

	return arg_parser.parse_args()


def preprocess(datasets):
	def preprocess_continuation(example):
		pronoun_idx = int(example['coreference_clusters'][-1])
		result = {
			'start': ' '.join(example['tokens'][:pronoun_idx]),
			'pronoun': example['tokens'][pronoun_idx],
			'continuation': ' '.join(example['tokens'][pronoun_idx+1:])
		}
		return result

	def preprocess_segment(example):
		text, start, continuation = '', '', ''
		pronoun_idx = int(example['coreference_clusters'][-1])
		pronoun = example['tokens'][pronoun_idx]

		for token_idx, token in enumerate(example['tokens']):
			prefix = ''
			if (token_idx > 0) and (token not in {'.', ',', '!', '?', ':', ';'}):
				prefix = ' '
			text += prefix + token
			if token_idx < pronoun_idx:
				start += prefix + token
			if token_idx > pronoun_idx:
				continuation += prefix + token

		return {
			'text': text,
			'start': start,
			'pronoun': pronoun,
			'continuation': continuation
		}

	for subset in datasets:
		datasets[subset] = datasets[subset].map(
			preprocess_segment,
			num_proc=4,
			remove_columns=datasets[subset].column_names
		)

	return datasets


def compute_perplexity(tokenizer, model, instance):
	with torch.no_grad():
		inputs = tokenizer(instance['text'], return_tensors='pt')
		inputs = {k: v.to(model.device) for k, v in inputs.items()}
		output = model(inputs['input_ids'], labels=inputs['input_ids'])  # labels are shifted automatically internally
	return float(output.loss.cpu())


def compute_continuation(tokenizer, model, instance, pronoun_ids):
	with torch.no_grad():
		inputs = tokenizer(instance['start'], return_tensors='pt')
		inputs = {k: v.to(model.device) for k, v in inputs.items()}
		output = model.generate(
			**inputs,
			do_sample=False,
			max_new_tokens=1,
			pad_token_id=tokenizer.pad_token_id,
			output_scores=True,
			# output_logits=True,  # logits == scores without sampling
			return_dict_in_generate=True
		)
	pred_continuation = tokenizer.decode(output['sequences'][0][-1])
	probabilities = torch.nn.functional.softmax(output['scores'][0].squeeze(), dim=0)
	target_pronoun_probability = float(probabilities[pronoun_ids[instance['pronoun']]].cpu())

	return pred_continuation, target_pronoun_probability


def main():
	args = parse_arguments()

	# load WinoBias dataset
	subsets = ['type1_anti', 'type1_pro', 'type2_anti', 'type2_pro']
	winobias = {}
	for subset in subsets:
		winobias[subset] = load_dataset('uclanlp/wino_bias', subset)[args.split]
	print(f"Loaded WinoBias subsets:\n{winobias}.")

	# preprocess into continuation format
	winobias= preprocess(winobias)
	# get all pronoun options in dataset
	pronouns = sorted(set(pronoun for dataset in winobias.values() for pronoun in dataset['pronoun']))
	print(f"Converted WinoBias into continuation format with pronoun options: {pronouns}.")

	# load existing tokenizer
	tokenizer = AutoTokenizer.from_pretrained(args.model, revision=args.model_revision, use_fast=True)
	tokenizer.pad_token = tokenizer.eos_token
	print(f"Loaded pre-trained tokenizer from '{args.model}' (revision={args.model_revision}).")

	# load existing model
	model = AutoModelForCausalLM.from_pretrained(args.model, revision=args.model_revision, device_map='auto')
	print(f"Loaded pre-trained model from '{args.model}' (revision={args.model_revision}):\n{model}")

	# extract token IDs for relevant pronouns
	pronoun_ids = {}
	for pronoun in pronouns:
		pronoun_ids[pronoun] = tokenizer(pronoun)['input_ids'][0]
	print(f"Identified token IDs for relevant pronouns: {pronoun_ids}.")

	# main evaluation loop
	results = {'type1': [], 'type2': []}
	for coref_type in range(1, 3):
		for instance_idx, (instance_pro, instance_anti) in enumerate(zip(winobias[f'type{coref_type}_pro'], winobias[f'type{coref_type}_anti'])):
			print(f"\x1b[1K\r[{coref_type} | {instance_idx + 1}] Computing perplexity...", end='', flush=True)
			# compute perplexity
			perplexity_pro = compute_perplexity(tokenizer, model, instance_pro)
			perplexity_anti = compute_perplexity(tokenizer, model, instance_anti)
			# compute continuations
			prediction_pro, pronoun_probability_pro = compute_continuation(tokenizer, model, instance_pro, pronoun_ids)
			prediction_anti, pronoun_probability_anti = compute_continuation(tokenizer, model, instance_anti, pronoun_ids)

			results[f'type{coref_type}'].append({
				'pro_perplexity': perplexity_pro,
				'pro_pronoun': instance_pro['pronoun'],
				'pro_pronoun_probability': pronoun_probability_pro,
				'pro_prediction': (prediction_pro == instance_pro['pronoun']),
				'anti_perplexity': perplexity_anti,
				'anti_pronoun': instance_anti['pronoun'],
				'anti_pronoun_probability': pronoun_probability_anti,
				'anti_prediction': (prediction_anti == instance_anti['pronoun'])
			})
		print(f"\x1b[1K\rComputed perplexity for {len(winobias[f'type{coref_type}_pro'])} pairs.")

	# export results
	with open(args.output, 'w') as fp:
		json.dump(results, fp)
	print(f"Saved results to '{args.output}'.")



if __name__ == '__main__':
	main()