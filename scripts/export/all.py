import argparse

from decaf.index import DecafIndex


def parse_arguments():
	parser = argparse.ArgumentParser(description="DECAF Index Exporter")
	parser.add_argument('--index', required=True, help='path to output DECAF index')
	parser.add_argument('--structure', required=True, help='specify structural level for export')
	parser.add_argument('--separator', default='\n', help='separator to place between structures')
	parser.add_argument('--output', help='path to output text file')
	return parser.parse_args()


def main():
	args = parse_arguments()
	print("="*22)
	print("📝️ DECAF Index Export")
	print("="*22)

	# connect to DECAF index
	decaf_index = DecafIndex(index_path=args.index)
	print(f"Connected to DECAF index at '{args.index}'.")

	# retrieve relevant structures
	structures = decaf_index.get_structures(stype=args.structure)
	print(f"Retrieved {len(structures)} {args.structure} structures.")

	# open output file
	output_file = None
	if args.output:
		output_file = open(args.output, 'w')

	# export relevant structures
	for export_idx, export in enumerate(decaf_index.export_structures(structures=structures)):
		if output_file:
			print(f"\x1b[1K\r[{export_idx + 1}/{len(structures)}] Exporting {args.structure} structures...", end='', flush=True)
			output_file.write(export + args.separator)
		else:
			print(export, end=args.separator)

	if output_file:
		output_file.close()

	print(f"\x1b[1K\rExported {len(structures)} {args.structure} structures.")


if __name__ == '__main__':
	main()
