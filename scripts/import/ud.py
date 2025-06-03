import argparse
import multiprocessing as mp
import time

from decaf.formats.conllu import ConlluBatcher, ConlluParser
from decaf.index import DecafIndex


#
# helper functions
#

def parse_arguments():
	parser = argparse.ArgumentParser(description="UD Importer")
	parser.add_argument('--input', required=True, help='path to UD treebank in CoNLL-U format')
	parser.add_argument('--output', required=True, help='path to output DECAF index')
	parser.add_argument('--literal-level', default='token', help='level at which to store atomic literals (default: character)')
	parser.add_argument('--force-alignment', action='store_true', default=False, help='set flag to force alignment between tokens and text (default: False)')
	parser.add_argument('--sentence-terminator', default=' ', help='terminator to add after each sentence (default: [space])')
	parser.add_argument('--commit-steps', type=int, help='number of steps after which to perform a backup commit (default: None)')
	parser.add_argument('--threads', type=int, default=mp.cpu_count(), help='number of workers (default: #CPUs)')
	parser.add_argument('--batch-size', type=int, default=1, help='number of sentences per batch, overriden by document boundaries (default: 1)')
	parser.add_argument('--shard-size', type=int, default=100000, help='number of sentences per shard (default: 100k)')
	return parser.parse_args()


#
# main
#

def main():
	args = parse_arguments()
	print("="*13)
	print("📥️ UD Import")
	print("="*13)

	# set up associated DECAF index
	decaf_index = DecafIndex(index_path=args.output)
	print(f"Connected to DECAF index at '{args.output}':")
	print(decaf_index)
	# initialize DECAF index
	decaf_index.initialize()
	print(f"Initialized index from scratch.")

	# initialize parser
	conllu_parser = ConlluParser(
		literal_level=args.literal_level,
		force_alignment=args.force_alignment,
		sentence_terminator=args.sentence_terminator
	)
	print(f"Loading UD treebank from '{args.input}'...", end='', flush=True)
	# count total number of sentences
	num_sentences = conllu_parser.get_size(file=args.input)
	print(f"found {num_sentences} sentence(s).")

	# initialize index-level variables
	cursor_idx = 0
	num_indexed_sentences = 0
	start_time = time.time()

	# process sentences in batches
	with ConlluBatcher(file=args.input) as batcher, decaf_index as di:
		for batch in batcher.get_batches(batch_size=args.batch_size):
			print(f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences/num_sentences:.2%}] Building index...", end='', flush=True)
			# parse batches
			batch_cursor, batch_literals, batch_structures, batch_hierarchies = conllu_parser.parse(sentences=batch)

			# update cursor to global offset
			for batch_literal in batch_literals:
				batch_literal.start += cursor_idx
				batch_literal.end += cursor_idx
			for batch_structure in batch_structures:
				batch_structure.start += cursor_idx
				batch_structure.end += cursor_idx
			# increment global cursor
			cursor_idx += batch_cursor

			# import into index
			di.add(literals=batch_literals, structures=batch_structures, hierarchies=batch_hierarchies)
			num_indexed_sentences += len(batch)

			# perform backup commit
			if (args.commit_steps is not None) and (num_indexed_sentences%args.commit_steps == 0):
				di.commit()
				print(f"\nPerformed backup commit to index at '{args.output}'.")

			# check if new shard should be created (batches respect document boundaries)
			if (num_indexed_sentences//args.shard_size) > len(di.shards):
				di.add_shard()

		# compute number of added structures
		num_literals, num_structures, num_hierarchies = di.get_size()
		end_time = time.time()

		print(
			f"\x1b[1K\rBuilt index with {len(di.shards)} shard(s) containing "
			f"{num_literals} literals "
			f"and {num_structures} structures "
			f"with {num_hierarchies} hierarchical relations "
			f"for {num_indexed_sentences} sentences "
			f"from '{args.input}' "
			f"in {end_time - start_time:.2f}s.")

	print(f"Saved updated DECAF index to '{args.output}'.")


if __name__ == '__main__':
	main()