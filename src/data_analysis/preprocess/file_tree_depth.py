import pandas as pd
import numpy as np


def compute_file_depths_per_mutation(df_mutations: pd.DataFrame):
    """
    Computes the file depth for each row in the input DataFrame.

    Args:
        df_mutations (pd.DataFrame): The input DataFrame containing file tree mutations.

    Returns:
        None. The "old_file_depth" "new_file_depth" columns are added to the input DataFrame in-place.
    """

    df_mutations["old_file_depth"] = (
        df_mutations["old_path"]
        .apply(lambda x: len(str(x).split("/")) - 1 if isinstance(x, str) else -1)
        .astype(np.int16)
    )
    df_mutations["new_file_depth"] = (
        df_mutations["new_path"]
        .apply(lambda x: len(str(x).split("/")) - 1 if isinstance(x, str) else -1)
        .astype(np.int16)
    )


def get_max_file_depth(df_mutations: pd.DataFrame):
    """
    Returns the maximum file depth of the given DataFrame.

    Args:
        df_mutations (pd.DataFrame): The DataFrame containing the file depth data.

    Returns:
        int: The maximum file depth.
    """
    return max(
        df_mutations["old_file_depth"].max(), df_mutations["new_file_depth"].max()
    )


def compute_files_per_level(
    df_commits: pd.DataFrame, df_mutations: pd.DataFrame, max_file_depth: int
) -> list[list[int]]:
    """
    Computes the number of files at each level of the file tree for each commit in a DataFrame of mutations.

    Args:
        df_mutations (pd.DataFrame): A DataFrame of mutations with columns "commit_id", "action", and "file_depth".
        max_file_depth (int): The maximum depth of the file tree.

    Returns:
        list[str]: A list of str, where each str is a comma separated list of numbers of files at each level of the file tree for a single commit.
    """
    # Initialize the list with a list of zeros
    nfiles_at_level_per_commit = [[0 for _ in range(max_file_depth + 1)]]

    for index, commit in df_commits.iterrows():
        new_nfiles_at_level = nfiles_at_level_per_commit[-1].copy()

        commit_muts = df_mutations[df_mutations["commit_id"] == index]
        for _, action in commit_muts.iterrows():
            if action["action"] == "add":
                new_nfiles_at_level[action["new_file_depth"]] += 1
            elif action["action"] == "delete":
                new_nfiles_at_level[action["old_file_depth"]] -= 1
            elif action["action"] == "move":
                new_nfiles_at_level[action["old_file_depth"]] -= 1
                new_nfiles_at_level[action["new_file_depth"]] += 1

        nfiles_at_level_per_commit.append(new_nfiles_at_level)

    return nfiles_at_level_per_commit[1:]
