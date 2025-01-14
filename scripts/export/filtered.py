import argparse
import time

import sqlparse

from decaf.index import DecafIndex, Criterion, Condition


def parse_arguments():
	parser = argparse.ArgumentParser(description="Filtered Index Exporter")
	parser.add_argument('--index', required=True, help='path to SQLite DECAF index')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*33)
	print("📝️ DECAF Filtered Index Export")
	print("="*33)

	# connect to DECAF index
	decaf_index = DecafIndex(db_path=args.index)

	# construct criterion
	constraint_level = 'sentence'
	output_level = 'sentence'
	# match structures of type "upos" with values "ADJ" or "NOUN"
	# constraint = Criterion(
	# 	conditions=[
	# 		Condition(stype='upos', values=['ADJ', 'NOUN'])
	# 	]
	# )
	# match structures of type "upos" with values "ADJ" with literal "second" and "NOUN"
	constraint = Criterion(
		operation='AND',
		conditions=[
			Condition(stype='upos', values=['ADJ'], literal="second"),
			Condition(stype='upos', values=['NOUN'])
		]
	)
	# constraint = Criterion(
	# 	operation='AND',
	# 	conditions=[
	# 		Condition(stype='upos', values=['ADJ', 'NOUN']),
	# 		Criterion(
	# 			conditions=[
	# 				Condition(stype='upos', values=['DET'])
	# 			]
	# 		)
	# 	]
	# )

	with decaf_index as di:
		num_atoms, num_structures = decaf_index.get_size()
		print(f"Connected to DECAF index at '{args.index}' with {num_atoms} atom(s) and {num_structures} structure(s).")

		print("Constructed SQL query from constraints:")
		print('```')
		print(sqlparse.format(di._construct_filter_query(
			constraint=constraint,
			constraint_level=constraint_level,
			output_level=output_level
		), reindent=True, keyword_case='upper'))
		print('```')
		print("Querying index...")
		query_start_time = time.time()

		# return all matching structures
		# outputs = di.filter(
		# 	constraint=constraint
		# )

		# return all sentences containing matching structures
		# outputs = di.filter(
		# 	constraint=constraint,
		# 	output_level=output_level
		# )

		# return all matching structures which occur together within one sentence
		# outputs = di.filter(
		# 	constraint=constraint,
		# 	constraint_level=constraint_level
		# )

		# return all matching structures which occur together within one sentence
		outputs = di.filter(
			constraint=constraint,
			constraint_level=constraint_level,
			output_level=output_level
		)

		num_matches = 0
		for sid, start, end, export in outputs:
			print(f"\n[ID {sid} | {start}-{end}] '{export}'")
			num_matches += 1

	print(
		f"\nCompleted retrieval of {num_matches} match(es) from DECAF index "
		f"with {num_atoms} atom(s) and {num_structures} structure(s) "
		f"in {time.time() - query_start_time:.2f}s."
	)


if __name__ == '__main__':
	main()
