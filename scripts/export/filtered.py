import argparse

from decaf.index import DecafIndex, Criterion, Condition


def parse_arguments():
	parser = argparse.ArgumentParser(description="Filtered Index Exporter")
	parser.add_argument('--index', required=True, help='path to output SQLite index')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*33)
	print("📝️ DECAF Filtered Index Export")
	print("="*33)

	# connect to DECAF index
	decaf_index = DecafIndex(db_path=args.index)
	print(f"Connected to DECAF index at '{args.index}'.")

	# construct criterion
	constraint = Criterion(
		conditions=[
			Condition(stype='upos', values=['ADJ', 'NOUN'])
		]
	)

	with decaf_index as di:
		# return all matching structures
		# outputs = di.filter_new(
		# 	constraint=constraint
		# )

		# return all sentences containing matching structures
		# outputs = di.filter_new(
		# 	constraint=constraint,
		# 	output_level='sentence'
		# )

		# return all matching structures which occur together within one sentence
		# outputs = di.filter_new(
		# 	constraint=constraint,
		# 	constraint_level='sentence'
		# )

		# return all matching structures which occur together within one sentence
		outputs = di.filter(
			constraint=constraint,
			constraint_level='sentence',
			output_level='sentence'
		)

		for sid, start, end, export in outputs:
			print(f"[ID {sid} | {start}-{end}] '{export}'\n")


if __name__ == '__main__':
	main()
