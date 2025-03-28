import argparse
import os
import time

import pandas as pd

from decaf.index import DecafIndex


def parse_arguments():
	parser = argparse.ArgumentParser(description="DECAF Annotation Overlap Analysis")
	parser.add_argument('--indices', nargs='+', required=True, help='path to DECAF index')
	parser.add_argument('--types', nargs='+', required=True, help='list of annotation types to compare')
	parser.add_argument('--output', help='path to export pickle file')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*37)
	print("🔬️ DECAF Annotation Overlap Analysis")
	print("="*37)

	# gather statistics for each index
	statistics = {}
	total_time = 0
	for index_path in args.indices:
		# connect to DECAF index
		decaf_index = DecafIndex(index_path=index_path)
		print(f"Loaded DECAF index at '{index_path}':\n{decaf_index}")

		# gather annotation statistics
		query_start_time = time.time()
		type_value_counts = decaf_index.get_structure_counts(types=args.types, values=True)
		total_time += (time.time() - query_start_time)
		print(f"Retrieved {len(type_value_counts)} annotation count(s) in {time.time() - query_start_time:.2f}s.")

		statistics[os.path.basename(index_path)] = type_value_counts

	statistics = pd.DataFrame.from_dict(statistics, orient='index')
	statistics = statistics.fillna(0)
	print(statistics)

	if args.output:
		statistics.to_pickle(args.output)
		print(f"Exported statistics to '{args.output}'.")

	print(f"Computed overlaps across {len(statistics)} indices and {len(statistics.columns)} in {total_time:.2f}s.")


if __name__ == '__main__':
	main()
