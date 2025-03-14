import argparse
import time

import sqlparse

from decaf.index import DecafIndex
from decaf.constraints import Constraint, Criterion, Condition


def parse_arguments():
	parser = argparse.ArgumentParser(description="Filtered Index Exporter")
	parser.add_argument('--index', required=True, help='path to SQLite DECAF index')
	parser.add_argument('--output', help='path to output file')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*33)
	print("📝️ DECAF Filtered Index Export")
	print("="*33)

	# connect to DECAF index
	decaf_index = DecafIndex(index_path=args.index)

	# construct criterion:
	# sentences containing tokens containing annotations matching
	# upos='DET' and surface form 'the
	# upos='ADJ'
	# upos='NOUN' and Number='Plur'
	# occurring in a sequence
	constraint = Constraint(
		criteria=[
			Criterion(
				conditions=[
					Condition(stype='upos', values=['DET'], literals=['the'])
				]
			),
			Criterion(
				conditions=[
					Condition(stype='upos', values=['ADJ'])
				]
			),
			Criterion(
				operation='AND',
				conditions=[
					Condition(stype='upos', values=['NOUN']),
					Condition(stype='Number', values=['Plur'])
				]
			)
		],
		sequential=True,
		hierarchy=['sentence', 'token']
	)
	output_level = 'structures'

	with decaf_index as di:
		num_atoms, num_structures, num_hierarchies = decaf_index.get_size()
		print(f"Connected to DECAF index at '{args.index}' with {num_atoms} atom(s) and {num_structures} structure(s) in {num_hierarchies} hierarchies.")

		print("Constructed SQL query from constraints:")
		print('```')
		sql = sqlparse.format(di._construct_filter_query(
			constraint=constraint,
			output_level=output_level
		), reindent=True, keyword_case='upper')
		print(sql)
		print('```')
		print("Querying index...")
		query_start_time = time.time()

		outputs = di.filter(constraint=constraint, output_level=output_level)

		# initialize output file pointer (if specified)
		output_file = None
		if args.output is not None:
			output_file = open(args.output, 'w', encoding='utf8')

		num_matches = 0
		for sid, start, end, export in outputs:
			print(f"\n[ID {sid} | {start}-{end}] '{export}'")
			if output_file is not None:
				output_file.write(export + '\n')
			num_matches += 1

		if output_file is not None:
			output_file.close()
			print(f"Saved {num_matches} outputs to '{args.output}'.")

	print(
		f"\nCompleted retrieval of {num_matches} match(es) from DECAF index "
		f"with {num_atoms} atom(s) and {num_structures} structure(s) "
		f"in {time.time() - query_start_time:.2f}s."
	)


if __name__ == '__main__':
	main()
