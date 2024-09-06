from data_analysis.commits_of_interest import (
    identify_commits_of_interest,
)

import pandas as pd


def extract_commits_of_interst(
    df_commits: pd.DataFrame, threshold: float = 0.8
) -> pd.DataFrame:
    # Identify commits of interest
    intr_cids = identify_commits_of_interest(
        df_commits, ["n_struc_changes"], threshold, detrend=False
    )
    return df_commits.loc[intr_cids]
