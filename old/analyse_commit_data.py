import pandas as pd
import argparse
from pathlib import Path
import matplotlib.pyplot as plt

args = argparse.ArgumentParser(description="Process input project folder.")
args.add_argument("input_datafolder", type=Path, help="input data folder")

args = args.parse_args()
input_datafolder: Path = args.input_datafolder
data_path = input_datafolder / "data.jsonl"
commits_path = input_datafolder / "commits.jsonl"

df_commits = pd.read_json(commits_path, lines=True)
df_data = pd.read_json(data_path, lines=True)

df_commits["commit_time"] = pd.to_datetime(df_commits["commit_time"])

df_commits["commit_time"].hist(bins=100)
plt.show()
