import pandas as pd
import numpy as np
from data_analysis.preprocess import (
    add_mutation_stats_per_commit,
    compute_file_depths_per_mutation,
    get_max_file_depth,
    compute_files_per_level,
)
import itertools


def preprocess(
    df_commits: pd.DataFrame, df_mutations: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    # Extract mutation statistics per commit
    add_mutation_stats_per_commit(df_commits, df_mutations)

    # Extract file depths
    compute_file_depths_per_mutation(df_mutations)
    max_file_depth = get_max_file_depth(df_mutations)

    # Extract files per level
    files_per_lvl_per_commit = compute_files_per_level(
        df_commits, df_mutations, max_file_depth
    )
    df_commits["total_files_per_level"] = list(
        map(lambda x: ",".join([str(i) for i in x]), files_per_lvl_per_commit)
    )
    df_commits["total_files_per_level"] = df_commits["total_files_per_level"].astype(
        pd.StringDtype()
    )

    df_commits["max_file_depth"] = list(
        map(
            lambda lst: len(lst)
            - len(list(itertools.takewhile(lambda x: x == 0, reversed(lst)))),
            files_per_lvl_per_commit,
        )
    )
    df_commits["max_file_depth"] = df_commits["max_file_depth"].astype(np.int16)
    df_commits["total_files"] = list(map(lambda x: sum(x), files_per_lvl_per_commit))
    df_commits["total_files"] = df_commits["total_files"].astype(np.int64)

    return df_commits, df_mutations, max_file_depth
