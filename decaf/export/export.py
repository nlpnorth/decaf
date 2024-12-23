import argparse

from decaf.index import DecafIndex


def parse_arguments():
	parser = argparse.ArgumentParser(description="Index Exporter")
	parser.add_argument('--index', required=True, help='path to output SQLite index')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*22)
	print("📝️ DECAF Index Export")
	print("="*22)

	# connect to DECAF index
	decaf_index = DecafIndex(db_path=args.index)
	print(f"Connected to DECAF index at '{args.index}'.")

	with decaf_index as di:
		num_atoms, num_structures = di.get_size()
		batch_size = 10000
		ranges = [(step-batch_size, step+1) for step in range(batch_size, num_atoms, batch_size)]
		if num_atoms%batch_size != 0:
			ranges.append((num_atoms - num_atoms%batch_size, num_atoms))
		for export_idx, export in enumerate(di.export_ranges(ranges=ranges)):
			print(f"[{ranges[export_idx][0]}-{ranges[export_idx][1]}] '{export}'\n")


if __name__ == '__main__':
	main()
