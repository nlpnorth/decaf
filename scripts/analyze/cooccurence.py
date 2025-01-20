import argparse
import time

from decaf.index import DecafIndex
from decaf.constraints import Constraint, Criterion, Condition


def parse_arguments():
	parser = argparse.ArgumentParser(description="DECAF Annotation Co-occurrence Analysis")
	parser.add_argument('--index', required=True, help='path to SQLite DECAF index')
	parser.add_argument('--source-types', nargs='+', help='list of source annotation types')
	parser.add_argument('--target-types', nargs='+', help='list of target annotation types')
	parser.add_argument('--constraint-level', help='level at which to apply constraints')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*43)
	print("🔬️ DECAF Annotation Co-occurrence Analysis")
	print("="*43)

	# connect to DECAF index
	decaf_index = DecafIndex(db_path=args.index)

	# set up types to compute co-occurrence for
	source_constraint = Constraint(
		criteria=[
			Criterion(
				operation='OR',
			    conditions=[Condition(stype=t) for t in args.source_types]
			)
		],
		level=args.constraint_level
	)
	target_constraint = Constraint(
		criteria=[
			Criterion(
				operation='OR',
			    conditions=[Condition(stype=t) for t in args.source_types]
			)
		],
		level=args.constraint_level
	)

	with decaf_index as di:
		# get overall structure statistics
		structure_counts = di.get_structure_counts()
		print("Available annotation types:")
		print(", ".join(structure_counts))
		print()
		print("Selected annotation types:")
		print(", ".join(args.source_types), "→", ", ".join(args.target_types))
		print()

		print("Querying index...")
		query_start_time = time.time()
		cooccurrence = di.get_cooccurence(
			source_constraint=source_constraint,
			target_constraint=target_constraint
		)
		print("Co-occurrence:")
		print(cooccurrence)

	print(
		f"\nComputed co-occurrence matrix for {cooccurrence.size} type pairs from DECAF index "
		f"in {time.time() - query_start_time:.2f}s."
	)


if __name__ == '__main__':
	main()
