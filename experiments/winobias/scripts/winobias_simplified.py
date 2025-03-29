import argparse
import math
import os
import pickle
import random
import time

from decaf.index import DecafIndex
from decaf.constraints import Filter, Criterion, Condition

# WinoBias constants
WINOBIAS_OCCUPATIONS = ['carpenter', 'editor', 'mechanician', 'designer', 'construction worker', 'accountant', 'laborer', 'auditor', 'driver', 'writer', 'sheriff', 'baker', 'mover', 'clerk', 'developer', 'cashier', 'farmer', 'counselor', 'guard', 'attendant' 'chief', 'teacher', 'janitor', 'sewer', 'lawyer', 'librarian', 'cook', 'assistant', 'physician', 'cleaner', 'CEO', 'housekeeper', 'analyst', 'nurse', 'manager', 'receptionist', 'supervisor', 'hairdresser', 'salesperson', 'secretary']


def parse_arguments():
	parser = argparse.ArgumentParser(description="WinoBias Filter")
	parser.add_argument('--indices', required=True, nargs='+', help='path to DECAF indices')
	parser.add_argument('--output', help='path to output files')
	parser.add_argument('--seed', type=int, default=42, help='seed for random components')
	return parser.parse_args()


def build_gendered_filters(gender):
	filters = {}

	# type 1/2
	# any gendered pronoun
	filters[(f'pron',)] = Filter(
		criteria=[
			Criterion(
				operation='AND',
				conditions=[
					Condition(stype='upos', values=['PRON']),
					Condition(stype='Gender', values=[gender])
				]
			)
		],
		sequential=True,
		hierarchy=['sentence', 'token']
	)

	return filters


def build_content_filters():
	filters = {}

	# type 1/2
	# occupational terms
	filters['occupations'] = Filter(
		criteria=[
			Criterion(
				conditions=[
					Condition(stype='token', literals=WINOBIAS_OCCUPATIONS)
				]
			)
		],
		hierarchy=['sentence']
	)

	return filters


def apply_filters(decaf_index, decaf_filters):
	filter_ranges = {}
	for filter_idx, (filter_name, decaf_filter) in enumerate(decaf_filters.items()):
		print(f"[{filter_idx+1}] Applying '{filter_name}' filter...", end='', flush=True)
		query_start_time = time.time()
		filter_ranges[filter_name] = set()
		for filter_range in decaf_index.get_filter_ranges(constraint=decaf_filter):
			filter_ranges[filter_name].add(filter_range)
		query_end_time = time.time()
		print(f"found {len(filter_ranges[filter_name])} matches in {query_end_time - query_start_time:.2f}s.")
	return filter_ranges


def order_by_specificity(content_ranges, positive_ranges, negative_ranges):
	def append_and_reduce(ranges, new_set):
		ranges.append(new_set)
		if len(ranges) > 1:
			ranges[-1] -= set.union(*ranges[:-1])
		print(f"[{len(ranges)}] Created filter range with {len(ranges[-1])} entries from {len(new_set)} ranges.")
		return ranges

	ordered_ranges = []

	# single-gender + pronoun + occupation
	ordered_ranges = append_and_reduce(
		ordered_ranges,
		positive_ranges[('pron',)] - negative_ranges[('pron',)] & content_ranges['occupations']
	)

	# single-gender + pronoun
	ordered_ranges = append_and_reduce(
		ordered_ranges,
		positive_ranges[('pron',)] - negative_ranges[('pron',)]
	)

	# mixed-gender + pronoun + occupation
	ordered_ranges = append_and_reduce(
		ordered_ranges,
		positive_ranges[('pron',)] & content_ranges['occupations']
	)

	# mixed-gender + pronoun
	ordered_ranges = append_and_reduce(
		ordered_ranges,
		positive_ranges[('pron',)]
	)

	specific_ranges, mixed_ranges = ordered_ranges[:len(ordered_ranges)//2], ordered_ranges[len(ordered_ranges)//2:]

	return specific_ranges, mixed_ranges


def extract_ordered_ranges(index_path, content_filters, f_filters, m_filters, output):
	# connect to DECAF index
	decaf_index = DecafIndex(index_path=index_path)
	print(f"Loaded DECAF Index: {decaf_index}.")

	# apply filters
	print("Retrieving all original sentences...")
	sentence_ranges = apply_filters(
		decaf_index,
		decaf_filters={'sentences': Filter([Criterion([Condition(stype='sentence')])])}
	)
	print(f"Identified {sum(len(r) for r in sentence_ranges.values())} sentences.")
	print("Applying content-based filters...")
	content_ranges = apply_filters(decaf_index, content_filters)
	print(f"Identified {sum(len(r) for r in content_ranges.values())} relevant content ranges.")
	print("Applying Fem-gendered filters...")
	f_ranges = apply_filters(decaf_index, f_filters)
	print(f"Identified {sum(len(r) for r in f_ranges.values())} Fem-gendered ranges.")
	print("Applying Masc-gendered filters...")
	m_ranges = apply_filters(decaf_index, m_filters)
	print(f"Identified {sum(len(r) for r in m_ranges.values())} Masc-gendered ranges.")
	print(f"Identified a total of {len(set.union(*f_ranges.values()) | set.union(*m_ranges.values()))} unique gendered ranges.")

	# print matched structures
	print_ranges(decaf_index, content_ranges)
	print_ranges(decaf_index, f_ranges)
	print_ranges(decaf_index, m_ranges)

	# order by specificity
	ordered_ranges_f, ordered_ranges_fm = order_by_specificity(content_ranges, f_ranges, m_ranges)
	ordered_ranges_m, ordered_ranges_mf = order_by_specificity(content_ranges, m_ranges, f_ranges)
	# merge mixed ranges
	ordered_ranges_x = [fm | mf for fm, mf in zip(ordered_ranges_fm, ordered_ranges_mf)]
	# get all non-gendered sentences
	other_ranges = [sentence_ranges['sentences'] - (set.union(*ordered_ranges_f) | set.union(*ordered_ranges_m) | set.union(*ordered_ranges_x))]
	# export ordered ranges
	ordered_ranges = {
		'f': ordered_ranges_f,
		'm': ordered_ranges_m,
		'x': ordered_ranges_x,
		'o': other_ranges
	}

	# export ranges
	index_output_path = os.path.join(output, os.path.basename(index_path))
	if not os.path.exists(index_output_path):
		os.mkdir(index_output_path)
	with open(os.path.join(index_output_path, 'ordered_ranges.pkl'), 'wb') as fp:
		pickle.dump(ordered_ranges, fp)
		print(f"Exported ordered ranges to '{os.path.join(index_output_path, 'ordered_ranges.pkl')}'.")

	return ordered_ranges


def print_ranges(decaf_index, filter_ranges):
	for filter_name, ranges in filter_ranges.items():
		print(f"Matches for '{filter_name}' (N={len(ranges)}):")
		for (shard_idx, structure_id, start, end), export in zip(ranges, decaf_index.export_ranges(sorted(ranges)[:10])):
			print(f"[ID ({shard_idx}/{structure_id}) | {start}-{end}] '{export}'")
		print("...")


def write_corpus(decaf_indices, ranges, corpus_path):
	structures_by_index = {i:[] for i in range(len(decaf_indices))}
	for index_idx, shard, structure, start, end in ranges:
		structures_by_index[index_idx].append((shard, structure))

	index_exports = {}
	for index_idx, index_structures in structures_by_index.items():
		print(f"[{index_idx}] Generating export from {decaf_indices[index_idx]}...", end='', flush=True)
		index_exports[index_idx] = {
			(shard, structure): export
			for (shard, structure), export in zip(index_structures, decaf_indices[index_idx].export_structures(index_structures))
		}
		print("done.")

	with open(corpus_path, 'w') as corpus_file:
		for line_idx, (index_idx, shard, structure, start, end) in enumerate(ranges):
			print(f"\x1b[1K\r[{line_idx + 1}/{len(ranges)} | {(line_idx + 1)/len(ranges):.2%}] Exporting corpus...", end='', flush=True)
			export = index_exports[index_idx][(shard, structure)]
			corpus_file.write(export + '\n')
	print("completed.")
	print(f"Exported corpus with {len(ranges)} total ranges to '{corpus_path}'.")


def export_ranges(index_paths, start_gender, end_gender, grouped_ranges, interleave_positions, output):
	decaf_indices = [DecafIndex(index_path=ip) for ip in index_paths]

	gendered_ranges = \
		[r for o in grouped_ranges[start_gender] for r in o] + \
		[r for o in grouped_ranges['x'] for r in o] + \
		[r for o in reversed(grouped_ranges[end_gender]) for r in o]
	other_ranges = grouped_ranges['o'][0]

	# generate final data order
	corpus_cursor, gendered_cursor, other_cursor = 0, 0, 0
	total_ranges = sum(sum(len(r) for r in o) for o in grouped_ranges.values())
	final_order = []
	while corpus_cursor < total_ranges:
		if corpus_cursor in interleave_positions:
			index_idx, shard, structure, start, end = gendered_ranges[gendered_cursor]
			gendered_cursor += 1
		else:
			index_idx, shard, structure, start, end = other_ranges[other_cursor]
			other_cursor += 1
		final_order.append((index_idx, shard, structure, start, end))
		corpus_cursor += 1
	output_order_path = os.path.join(output, f'corpus-order-{start_gender}-{end_gender}.pkl')
	with open(output_order_path, 'wb') as fp:
		pickle.dump(final_order, fp)
	print(f"Exported final data order to '{output_order_path}'.")

	# export ordered data
	write_corpus(decaf_indices, final_order, os.path.join(output, f'corpus-{start_gender}-{end_gender}.txt'))


def main():
	args = parse_arguments()
	random.seed(args.seed)

	# construct WinoBias filters
	f_filters = build_gendered_filters('Fem')
	m_filters = build_gendered_filters('Masc')
	content_filters = build_content_filters()

	# gather ordered ranges from each index
	ordered_ranges = {'f': [], 'm': [], 'x': [], 'o': []}
	for index_idx, index_path in enumerate(args.indices):
		index_name = os.path.basename(index_path)
		# if available, load ordered ranges
		ranges_path = os.path.join(args.output, os.path.basename(index_path), 'ordered_ranges.pkl')
		if os.path.exists(ranges_path):
			with open(ranges_path, 'rb') as fp:
				cur_ordered_ranges = pickle.load(fp)
			print(f"Loaded existing ordered ranges from '{ranges_path}'.")
		# compute ranges from scratch
		else:
			cur_ordered_ranges = extract_ordered_ranges(index_path, content_filters, f_filters, m_filters, args.output)
		# order ranges and prefix with index name
		print(f"Extracted ordered ranges from '{index_name}':")
		for category in ordered_ranges:
			for specificity, ranges in enumerate(cur_ordered_ranges[category]):
				if specificity >= len(ordered_ranges[category]):
					ordered_ranges[category].append([])
				ordered_ranges[category][specificity] += [(index_idx,) + r for r in sorted(ranges)]
			print(f"  * {category}: {sum(len(r) for r in cur_ordered_ranges[category])}")

	# balance pronouns in each specificity level
	print("Balancing pronoun proportions in each specificity level:")
	for specificity in range(len(ordered_ranges['f'])):
		f_size, m_size = len(ordered_ranges['f'][specificity]), len(ordered_ranges['m'][specificity])
		min_size = min(f_size, m_size)
		if f_size >= min_size:
			ordered_ranges['f'][specificity] = random.sample(ordered_ranges['f'][specificity], min_size)
		if m_size >= min_size:
			ordered_ranges['m'][specificity] = random.sample(ordered_ranges['m'][specificity], min_size)
		print(f"[{specificity}] Sampled {f_size} -> {min_size} (f) and {m_size} -> {min_size} (m).")

	print("Sizes for each specificity level:")
	for category in ordered_ranges:
		for specificity, ranges in enumerate(ordered_ranges[category]):
			print(f"  * {category}/{specificity}: {len(ranges)}")

	# get interleave order
	total_sentences = sum(sum(len(s) for s in r) for r in ordered_ranges.values())
	total_gendered = total_sentences - len(ordered_ranges['o'][0])
	insert_steps = total_sentences // total_gendered
	interleave_positions = list(range(0, total_sentences, insert_steps))
	# even out the interleave
	if len(interleave_positions) > total_gendered:
		overhang = len(interleave_positions) - total_gendered
		interleave_positions = interleave_positions[:len(interleave_positions)//2 - overhang//2] + interleave_positions[len(interleave_positions)//2 + math.ceil(overhang/2):]
	interleave_positions = set(interleave_positions)
	print(f"Interleaving a total of {total_gendered} gendered sentences every {insert_steps} steps across {total_sentences} total sentences.")

	# shuffle order within each data category
	for category in ordered_ranges:
		for specificity in range(len(ordered_ranges[category])):
			random.shuffle(ordered_ranges[category][specificity])

	# export final datasets
	export_ranges(args.indices, 'f', 'm', ordered_ranges, interleave_positions, args.output)
	export_ranges(args.indices, 'm', 'f', ordered_ranges, interleave_positions, args.output)


if __name__ == '__main__':
	main()
