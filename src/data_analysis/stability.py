from math import ceil, floor
import pandas as pd
import numpy as np


# def compute_depth_stability(df_commit: pd.DataFrame) -> float:
#     """
#     Computes the depth stability score for a given DataFrame of commits.

#     Args:
#         df_commit (pd.DataFrame): DataFrame of commits with a "max_file_depth" column.

#     Returns:
#         float: The depth stability score, which is the proportion of time where the max file depth is stable
#         (changes are less than or equal to 0 in the rolling mean). A score closer to 1 indicates a higher proportion
#         of time where the max file depth is stable. A lower score suggests more dynamic changes in the max file depth.
#     """
#     max_file_depth_changes = df_commit["max_file_depth"].diff().abs()
#     window = int(len(df_commit) * 0.02)
#     rolling_mean_changes = max_file_depth_changes.rolling(window).mean()
#     stability_score = (rolling_mean_changes <= 0).sum() / len(
#         rolling_mean_changes.dropna()
#     )
#     return stability_score


def compute_structural_change_stability(
    df_commit: pd.DataFrame, time_interval: str = "monthly"
) -> float:
    """
    Computes the structural change stability score for a given DataFrame of commits.

    Args:
        df_commit (pd.DataFrame): DataFrame of commits with relevant columns.
        time_interval (str): Time interval for analyzing changes ('monthly', 'quarterly', 'yearly').

    Returns:
        float: The structural change stability score based on entropy or dispersion metric.
    """
    # Ensure commit_time is in datetime format
    df_commit["commit_time"] = pd.to_datetime(df_commit["commit_time"], unit="s")

    # Define time interval based on time_interval parameter
    if time_interval == "daily":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("D")
    elif time_interval == "monthly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("M")
    elif time_interval == "quarterly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("Q")
    elif time_interval == "yearly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("Y")
    else:
        raise ValueError(
            "Invalid time_interval. Use 'monthly', 'quarterly', or 'yearly'."
        )

    # Aggregate structural change counts per time interval
    changes_per_period = (
        df_commit.groupby(["repo_id", "time_period"])["n_struc_changes"]
        .sum()
        .reset_index()
    )

    # Calculate probabilities of structural changes per time interval
    total_changes_per_period = changes_per_period.groupby("time_period")[
        "n_struc_changes"
    ].sum()
    probabilities = changes_per_period["n_struc_changes"] / changes_per_period[
        "time_period"
    ].map(total_changes_per_period)

    # Calculate entropy (Shannon entropy) based on change probabilities
    entropy = -(probabilities * np.log2(probabilities)).sum()

    # Alternatively, calculate dispersion (e.g., variance) of structural changes
    # dispersion = changes_per_period['n_struc_changes'].var()

    # Normalize the metric if needed
    # normalized_entropy = (entropy - min_entropy) / (max_entropy - min_entropy)

    return entropy


def compute_depth_stability(
    df_commit: pd.DataFrame, time_interval: str = "monthly"
) -> float:
    """
    Computes the stability of max file depth over time for a given DataFrame of commits.

    Args:
        df_commit (pd.DataFrame): DataFrame of commits with relevant columns.
        time_interval (str): Time interval for analyzing changes ('daily', 'monthly', 'quarterly', 'yearly').

    Returns:
        float: The stability score of max file depth over the specified time interval.
    """
    # Ensure commit_time is in datetime format
    df_commit["commit_time"] = pd.to_datetime(df_commit["commit_time"], unit="s")

    # Define time interval based on time_interval parameter
    if time_interval == "daily":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("D")
    elif time_interval == "monthly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("M")
    elif time_interval == "quarterly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("Q")
    elif time_interval == "yearly":
        df_commit["time_period"] = df_commit["commit_time"].dt.to_period("Y")
    else:
        raise ValueError(
            "Invalid time_interval. Use 'daily', 'monthly', 'quarterly', or 'yearly'."
        )

    # Calculate the maximum file depth per time interval
    max_depth_per_period = df_commit.groupby(["repo_id", "time_period"])[
        "max_file_depth"
    ].max()

    # Calculate stability score based on changes in max file depth
    depth_changes = max_depth_per_period.diff().abs()
    stability_score = (depth_changes <= 0).sum() / len(depth_changes.dropna())

    return stability_score
