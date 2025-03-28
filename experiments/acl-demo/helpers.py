import itertools
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.spatial.distance import jensenshannon
import sqlparse


def count_filtered(decaf_index, decaf_filter, print_sql=False, print_results=False, output_level='structures'):
    query_start_time, query_end_time = time.time(), None
    if print_sql:
        print("The constructed SQL query is:")
        print("```sql")
        print(sqlparse.format(
        decaf_index._construct_filter_query(
            constraint=decaf_filter,
            output_level=output_level
        ), reindent=True, keyword_case='upper'))
        print("```")

    # return all matching structures within their containing sentence
    outputs = decaf_index.filter(
        constraint=decaf_filter,
        output_level=output_level
    )

    structure_counts = {}
    num_matches = 0
    for shard_idx, structure_idx, start, end, export in outputs:
        if query_end_time is None: query_end_time = time.time()
        structure_counts[export] = structure_counts.get(export, 0) + 1
        if print_results:
            print(f"[ID {(shard_idx, structure_idx)} | {start}-{end}] '{export}'")
        num_matches += 1

    print(
        f"\nCompleted retrieval of {num_matches} match(es) from DECAF index "
        f"in {query_end_time - query_start_time:.2f}s."
    )
    return structure_counts


def plot_frequency(structure_counts):
    structures, counts = zip(*sorted(structure_counts.items(), key=lambda i: i[1], reverse=True))
    fig, ax = plt.subplots(figsize=(15, 6.3 * .6))
    ax.bar(structures, counts, color='sienna')
    ax.set_xticks(range(len(structures)), structures, rotation=45, ha='right')
    ax.set_xlim(-1, len(structures)+1)
    plt.show()


def compute_divergence(statistics, total_sizes):
    statistics = pd.DataFrame.from_dict(statistics, orient='index')
    statistics = statistics.fillna(0)
    # add 'other' columns to single-value groups
    group_values = {}
    for (group, value) in statistics.columns:
        group_values[group] = group_values.get(group, []) + [value]

    for group, values in group_values.items():
        if len(values) > 1:
            continue
        statistics[(group, 'Other')] = [t - c for t, c in zip(total_sizes.values(), statistics[(group, values[0])])]

    statistics = statistics.reindex(sorted(statistics.columns), axis=1)

    # normalize counts into distributions
    normalized = statistics.copy().astype(float)
    group_totals = statistics.T.groupby(level=0).sum().T

    for (group, value) in normalized.columns:
        normalized[(group, value)] /= group_totals[group]

    # compute JS divergence within each group
    divergence = np.zeros((len(normalized), len(normalized)))
    divergence_by_comparison = {}

    # iterate over groups
    for group, group_frame in normalized.T.groupby(level=0):
        # iterate over all pairs of rows
        for i, j in itertools.combinations(range(len(normalized)), 2):
            p = group_frame.T.iloc[i].values
            q = group_frame.T.iloc[j].values

            js = jensenshannon(p, q)

            # store individual comparison
            divergence_by_comparison[(group_frame.T.index[i], group_frame.T.index[j], group)] = float(js)

            # fill the symmetric matrix
            divergence[i, j] += js
            divergence[j, i] += js

    # take mean over all groups
    divergence /= len(group_totals)

    return divergence, divergence_by_comparison, statistics


def plot_confusions(divergence, labels, output=None):
    fig, ax = plt.subplots(figsize=(6.3, 6.3))

    # Plot the heatmap
    im = ax.imshow(divergence, cmap=plt.get_cmap('YlOrBr'))

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, shrink=0.8)
    cbar.ax.set_ylabel('JS Divergence', rotation=-90, va="bottom")

    # set tick labels
    ax.set_xticks(np.arange(len(divergence)))
    ax.set_yticks(np.arange(len(divergence)))
    ax.set_xticklabels(labels[r] for r in range(divergence.shape[0]))
    ax.set_yticklabels(labels[r] for r in range(divergence.shape[0]))

    # Rotate the tick labels and set their alignment
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

    # Turn spines off and create white grid
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    ax.set_xticks(np.arange(divergence.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(divergence.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    # Add text annotations with values
    thresh = np.max(divergence) / 2.
    for i in range(len(divergence)):
        for j in range(len(divergence)):
            value = divergence[i, j]
            text_color = "white" if value > thresh else "black"
            ax.text(j, i, f"{value:.2f}", ha="center", va="center", color=text_color)

    fig.tight_layout()
    if output is not None:
        plt.savefig(output, bbox_inches='tight', pad_inches=.05)
    plt.show()
