from sklearn.ensemble import IsolationForest
import pandas as pd


import pandas as pd
from typing import List
from sklearn.ensemble import IsolationForest


def identify_commits_of_interest(
    data: pd.DataFrame,
    columns: List[str],
    threshold: float = 0.6,
    detrend: bool = False,
) -> List[pd.Index]:
    """
    Identifies commits of interest based on anomaly detection using Isolation Forest.

    Args:
        data (pd.DataFrame): The input data to analyze.
        columns (List[str]): The columns to use for anomaly detection.
        threshold (float, optional): The threshold for anomaly detection. Defaults to 0.6.
        detrend (bool, optional): Whether to detrend the data before anomaly detection. Defaults to False.

    Returns:
        List[pd.Index]: A list of indexes of commits of interest.
    """
    tmp_data = data.copy()
    if detrend:
        # Detrend -> Software systems grow over time
        tmp_data[columns] = tmp_data[columns].diff().fillna(0)

    isolation_forest = IsolationForest(contamination="auto", random_state=42)
    isolation_forest.fit(tmp_data[columns])

    anomaly_scores = isolation_forest.score_samples(tmp_data[columns])
    outlier_indexes = tmp_data[anomaly_scores <= -threshold].index.to_list()
    return outlier_indexes
