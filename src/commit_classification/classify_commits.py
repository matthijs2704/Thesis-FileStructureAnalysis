from typing import List
from simpletransformers.classification import MultiLabelClassificationModel
import pandas as pd
from pathlib import Path
import torch


def classify_commits(input_file: Path, output_file: Path):
    df = pd.read_json(input_file, lines=True)
    commit_msgs = df["message"].str.lower().tolist()
    predictions = classify_commit_messages(commit_msgs)
    df[["corrective", "adaptive", "perfective"]] = predictions
    df.to_json(path_or_buf=output_file, orient="records", lines=True)


def classify_commit_messages(commit_msgs: List[str]) -> List[List[int]]:
    if len(commit_msgs) == 0:
        return []

    is_cuda_available = torch.cuda.is_available()

    # Load TransformerModel
    model = MultiLabelClassificationModel(
        "distilbert",
        "commit_classification/model/",
        args={},
        use_cuda=is_cuda_available,
    )

    # commit_msgs = df["message"].str.lower().tolist()
    predictions, raw_outputs = model.predict(commit_msgs)
    return predictions
    # df[["corrective", "adaptive", "perfective"]] = predictions

    # print(df.head())
    # df.to_json(
    #     path_or_buf="../output/commits_classified.jsonl", orient="records", lines=True
    # )
