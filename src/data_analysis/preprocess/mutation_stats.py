import pandas as pd
import numpy as np


def add_mutation_stats_per_commit(df_commit: pd.DataFrame, df_mutations: pd.DataFrame):
    """
    Extracts mutation statistics per commit from a DataFrame of file-level mutations.

    Args:
        df (pd.DataFrame): A DataFrame of file-level mutations, with columns "commit_id",
            "file_path", "action".
        df_commit (pd.DataFrame): A DataFrame of commits, with a column "commit_id".

    Returns:
        None. The function modifies the input df_commit DataFrame in place, adding columns
        "n_added", "n_deleted", "n_moved", and "n_renamed" that contain the number of added,
        deleted, moved, and renamed lines of code per commit, respectively.
    """
    df_commit["n_added"] = (
        df_mutations[df_mutations["action"] == "add"].groupby("commit_id").size()
    )

    df_commit["n_deleted"] = (
        df_mutations[df_mutations["action"] == "delete"].groupby("commit_id").size()
    )

    df_commit["n_moved"] = (
        df_mutations[df_mutations["action"] == "move"].groupby("commit_id").size()
    )

    df_commit["n_renamed"] = (
        df_mutations[df_mutations["action"] == "rename"].groupby("commit_id").size()
    )

    df_commit.fillna(
        {"n_added": 0, "n_deleted": 0, "n_moved": 0, "n_renamed": 0}, inplace=True
    )

    df_commit["n_added"] = df_commit["n_added"].astype(np.int32)
    df_commit["n_deleted"] = df_commit["n_deleted"].astype(np.int32)
    df_commit["n_moved"] = df_commit["n_moved"].astype(np.int32)
    df_commit["n_renamed"] = df_commit["n_renamed"].astype(np.int32)

    df_commit["n_struc_changes"] = (
        df_commit["n_added"]
        + df_commit["n_deleted"]
        + df_commit["n_moved"]
        + df_commit["n_renamed"]
    ).astype(np.int64)
