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
    stats = {p:0 for p in ['batching', 'batching/get', 'parsing', 'parsing/apply', 'gathering', 'gathering/ready', 'gathering/get', 'adding', 'db']}

    # open contexts for database, data parsing, and parallel processing
    with decaf_index as di, ConlluBatcher(file=args.input) as batcher, mp.Pool(processes=args.threads) as pool:
        # get batch iterator
        batch_iterator = batcher.get_batches(batch_size=args.batch_size)
        batch_idx = 0
        batches = []
        batch_sizes = {}
        batch_results = {}
        output_idx = 0

        # main parallel processing loop
        parsing_jobs = {}
        while num_indexed_sentences < num_sentences:
            # print(f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'waiting' status...", end='', flush=True)
            # buffer future batches
            batching_start_time = time.time()
            while (batch_iterator is not None) and (len(batches) < args.threads * 2):
                print(
                    # f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'batching' status...",
                    end='', flush=True)
                try:
                    batching_get_start_time = time.time()
                    batch = next(batch_iterator)
                    batches.append((batch_idx, batch))
                    batch_sizes[batch_idx] = len(batch)
                    batch_idx += 1
                    stats['batching/get'] += time.time() - batching_get_start_time
                # stop adding jobs after batches are depleted
                except StopIteration:
                    batch_iterator = None
                    break
            stats['batching'] += time.time() - batching_start_time

            # submit new parsing jobs up to pool size
            parsing_start_time = time.time()
            while batches and (len(parsing_jobs) < args.threads):
                print(
                    # f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'submitting' status...",
                    end='', flush=True)
                parsing_apply_start_time = time.time()
                job_batch_idx, batch = batches.pop(0)
                parsing_job = pool.apply_async(conllu_parser.parse, (batch,))
                parsing_jobs[job_batch_idx] = parsing_job
                stats['parsing/apply'] += time.time() - parsing_apply_start_time
            stats['parsing'] += time.time() - parsing_start_time

            # gather completed jobs
            gathering_start_time = time.time()
            for job_batch_idx in list(parsing_jobs.keys()):
                batch_job = parsing_jobs[job_batch_idx]
                # check if job completed
                gathering_ready_start_time = time.time()
                batch_job_ready = batch_job.ready()
                stats['gathering/ready'] += time.time() - gathering_ready_start_time
                if not batch_job_ready:
                    continue
                # process results
                print(
                    # f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'gathering' status...",
                    end='', flush=True)
                try:
                    gathering_get_start_time = time.time()
                    batch_results[job_batch_idx] = batch_job.get()
                    stats['gathering/get'] += time.time() - gathering_get_start_time
                    # remove from active jobs
                    del parsing_jobs[job_batch_idx]
                except Exception as exception:
                    print(f"[Error] Could not process batch {job_batch_idx}:\n{exception}")
                    raise exception
            stats['gathering'] += time.time() - gathering_start_time

            # process results in target output order
            adding_start_time = time.time()
            literals, structures, hierarchies = [], [], []
            while output_idx in batch_results:
                # print(f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'adding' status...", end='', flush=True)
                # get relevant result
                batch_cursor, batch_literals, batch_structures, batch_hierarchies = batch_results.pop(output_idx)

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
                literals += batch_literals
                structures += batch_structures
                hierarchies += batch_hierarchies
                num_indexed_sentences += batch_sizes[output_idx]
                del batch_sizes[output_idx]
                output_idx += 1
            stats['adding'] += time.time() - adding_start_time

            db_start_time = time.time()
            if literals and structures and hierarchies:
                print(
                    f"\x1b[1K\r[{num_indexed_sentences}/{num_sentences} | {num_indexed_sentences / num_sentences:.2%}] Building index across {len(parsing_jobs)}/{args.threads} threads, {len(batches)} inputs queued, {len(batch_results)} results queued, 'db' status...",
                    end='', flush=True)

                # perform backup commit
                if (args.commit_steps is not None) and (num_indexed_sentences % args.commit_steps == 0):
                    di.commit()
                    print(f"\nPerformed backup commit to index at '{args.output}'.")

                # check if new shard should be created (batches respect document boundaries)
                if (num_indexed_sentences // args.shard_size) > len(di.shards):
                    di.add_shard()

                # add processed data
                di.add(literals=literals, structures=structures, hierarchies=hierarchies)
            stats['db'] += time.time() - db_start_time

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

    print(f"STATISTICS:\n{stats}")

    print(f"Saved updated DECAF index to '{args.output}'.")


if __name__ == '__main__':
    main()