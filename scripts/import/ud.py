import argparse
import copy
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
    parser.add_argument('--shard-size', type=int, default=10000, help='number of sentences per shard (default: 10k)')
    return parser.parse_args()


def gen_shard_batchers(conllu_file, batch_size, shard_size):
    sentence_idx = 0
    shard_start = 0
    num_shards = 0
    with ConlluBatcher(file=conllu_file) as batcher:
        for batch in batcher.get_batches(batch_size=batch_size):
            # check if new shard should be created (batches respect document boundaries)
            if (sentence_idx // shard_size) > num_shards:
                yield num_shards, sentence_idx, ConlluBatcher(file=conllu_file, start=shard_start, end=sentence_idx)
                shard_start = sentence_idx + 1
                num_shards += 1
            sentence_idx += len(batch)
        if num_shards > 0:
            yield num_shards, sentence_idx, ConlluBatcher(file=conllu_file, start=shard_start, end=sentence_idx)
            num_shards += 1


def shard_worker(decaf_index, conllu_batcher, conllu_parser, shard, batch_size):
    cursor_idx = 0
    sentence_idx = 0
    shard_literal_ids = []
    shard_structure_ids = []

    # connect to specified shard
    decaf_index.connect(shard=shard)
    # open file pointer
    with conllu_batcher as batcher:
        # iterate over batches
        for batch in batcher.get_batches(batch_size=batch_size):
            # parse batch
            batch_cursor, batch_literals, batch_structures, batch_hierarchies = conllu_parser.parse(batch, cursor_idx=cursor_idx)

            # write to database at relevant shard
            decaf_index.add(literals=batch_literals, structures=batch_structures, hierarchies=batch_hierarchies)

            # gather literal/structure IDs
            shard_literal_ids += [literal.id for literal in batch_literals]
            shard_structure_ids += [structure.id for structure in batch_structures]
            # update global cursors
            cursor_idx = batch_cursor
            sentence_idx += len(batch)
    # close database connection
    decaf_index.disconnect()
    del decaf_index

    return cursor_idx, sentence_idx, shard_literal_ids, shard_structure_ids


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

    # initialize parser
    conllu_parser = ConlluParser(
        literal_level=args.literal_level,
        force_alignment=args.force_alignment,
        sentence_terminator=args.sentence_terminator
    )

    # initialize sharding
    print(f"Loading UD treebank from '{args.input}'...", end='', flush=True)
    shard_batchers = gen_shard_batchers(
        conllu_file=args.input, batch_size=args.batch_size, shard_size=args.shard_size
    )

    # main parallel processing loop
    shard_workers = {}
    shard_results = {}
    num_shards = 0
    num_sentences = 0
    num_indexed_sentences = 0
    start_time = time.time()
    with mp.Pool(processes=args.threads) as pool:
        # process until all sentences have been indexed
        while shard_batchers or (num_indexed_sentences < num_sentences):
            # submit shard processing jobs
            if shard_batchers:
                # print initial progress
                print(f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences}] Loading dataset into {num_shards} shard(s)...", end='', flush=True)
                # gather next shard batcher
                try:
                    shard_idx, shard_sentence_idx, shard_batcher = next(shard_batchers)
                    num_shards = int(shard_idx+1)
                    num_sentences = int(shard_sentence_idx)

                    # initialize new shard
                    decaf_index.add_shard()
                    decaf_index.disconnect()

                    # submit shard processing to pool
                    shard_job = pool.apply_async(
                        shard_worker,
                        (copy.deepcopy(decaf_index), copy.deepcopy(shard_batcher), copy.deepcopy(conllu_parser), shard_idx, args.batch_size)
                    )
                    shard_workers[shard_idx] = shard_job
                # clear generator after batcher exhaustion
                except StopIteration:
                    shard_batchers = None

            # gather completed jobs
            for job_shard_idx in list(shard_workers.keys()):
                shard_job = shard_workers[job_shard_idx]
                # check if job completed
                if not shard_job.ready():
                    continue
                # process results
                try:
                    # retrieve results
                    shard_results[job_shard_idx] = shard_job.get()
                    # increment statistics
                    num_indexed_sentences += shard_results[job_shard_idx][1]
                    # remove from active jobs
                    del shard_workers[job_shard_idx]
                    # print progress
                    print(f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index with {num_shards} shard(s)...", end='', flush=True)
                except Exception as exception:
                    print(f"[Error] Could not process shard {job_shard_idx}:\n{exception}")
                    raise exception

    # compute number of added structures
    num_literals, num_structures, num_hierarchies = decaf_index.get_size()
    end_time = time.time()

    print(
        f"\x1b[1K\rBuilt index with {len(decaf_index.shards)} shard(s) containing "
        f"{num_literals} literals "
        f"and {num_structures} structures "
        f"with {num_hierarchies} hierarchical relations "
        f"for {num_indexed_sentences} sentences "
        f"from '{args.input}' "
        f"in {end_time - start_time:.2f}s.")

    print(f"Saved updated DECAF index to '{args.output}'.")


if __name__ == '__main__':
    main()