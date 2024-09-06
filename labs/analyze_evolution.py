import argparse
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import json
import itertools
import plotly.graph_objects as go

args = argparse.ArgumentParser(description="Process input project folder.")
args.add_argument("input_datafolder", type=Path, help="input data folder")
args = args.parse_args()
input_datafolder: Path = args.input_datafolder

data_path = input_datafolder / "data.jsonl"
commits_path = input_datafolder / "commits.jsonl"
all_commits_path = input_datafolder / "all_commits.csv"
repo_info_path = input_datafolder / "repo_info.json"

# Load the data
all_commits = pd.read_csv(all_commits_path)
all_commit_ids = all_commits["commit_id"].tolist()

with open(repo_info_path, "r") as json_file:
    repo_info: dict = json.load(json_file)
df = pd.read_json(data_path, lines=True)
df_commit = pd.read_json(commits_path, lines=True).set_index("commit_id")

# Combine the datasets
df = df.join(df_commit, on="commit_id")

# Sort both by commit time
df = df.sort_values("commit_time", ascending=True)
df_commit = df_commit.sort_values("commit_time", ascending=True)
max_file_depth = repo_info["max_file_depth"]


# Stability, by rolling average of max file depth changes
# The lower the score, the more stable the project is
from data_analysis import compute_depth_stability

depth_stability = compute_depth_stability(df_commit) * 100
print(f"Depth stability score: {depth_stability:.2f}")

# exit(0)
# region Plot max file depth over time
# plt.plot(df_commit.index, df_commit["max_file_depth"])
# plt.xticks(np.arange(0, len(df_commit), len(df_commit) // 50), rotation=90)
# plt.xlabel("Commit ID")
# plt.ylabel("Max File Depth")
# plt.title("Evolution of Max File Depth over Time")
# plt.show()

# Create a Plotly figure
fig = go.Figure()

# Add a line to the figure
fig.add_trace(
    go.Scatter(
        x=df_commit.index,
        y=df_commit["max_file_depth"],
        mode="lines",
        name="Max File Depth",
    )
)

# Set the x-axis and y-axis labels and the title
fig.update_layout(
    xaxis_title="Commit ID",
    yaxis_title="Max File Depth",
    title=f"Evolution of Max File Depth over Time (stability score: {depth_stability:.2f})",
)

# Show the figure
fig.show()
# endregion
exit(0)

# region Plot total number of files over time
plt.plot(df_commit.index, df_commit["nfiles"])
plt.xticks(np.arange(0, len(df_commit), len(df_commit) // 50), rotation=90)
plt.xlabel("Commit ID")
plt.ylabel("Number of Files")
plt.title("Evolution of Number of Files over Time")
plt.show()
# endregion

# region Plot the number of files in each level over time
# Convert the list of values to a numpy array
num_files_in_lvl_per_commit = np.array(df_commit["nfiles_per_level"].tolist())

# Prepare colors for plotting
cm = plt.get_cmap("tab20")
colors = cm(np.linspace(0, 1, max_file_depth + 1))

if True:
    # Create a line plot of num_files_in_lvl_per_commit over time
    for i in range(max_file_depth + 1):
        plt.plot(df_commit.index, num_files_in_lvl_per_commit[:, i], color=colors[i])
    plt.xticks(np.arange(0, len(df_commit), 100.0), rotation=90)
    plt.xlabel("Commit ID")
    plt.ylabel("Number of Files in Each Level")
    plt.legend([f"Level {x}" for x in range(max_file_depth + 1)])
    plt.title("Evolution of Number of Files in Each Level over Time")
    plt.show()

if False:
    window_size = 100
    # Calculate the moving average of files in each level by window_size commit window
    moving_avg = np.apply_along_axis(
        lambda x: np.convolve(x, np.ones(window_size), mode="valid") / window_size,
        axis=0,
        arr=num_files_in_lvl_per_commit,
    )

    commit_idx_per_window = np.arange(0, len(df_commit), window_size)
    final_nfiles_per_level = num_files_in_lvl_per_commit[-1]

    # Create a line plot of moving average of num_files_in_lvl_per_commit over time
    for i in range(max_file_depth + 1):
        plt.plot(
            df_commit.index[window_size - 1 :],
            moving_avg[:, i],
            color=colors[i % len(colors)],
        )

    plt.xticks(commit_idx_per_window, rotation=90)
    plt.xlabel("Commit ID")
    plt.ylabel("Moving Average of Number of Files in Each Level")
    plt.legend([f"Level {x}" for x in range(max_file_depth + 1)])
    plt.title("Evolution of Moving Average of Number of Files in Each Level over Time")
    for i in range(max_file_depth + 1):
        plt.plot(
            [
                0,
                commit_idx_per_window[-1] + window_size,
            ],
            [
                0,
                final_nfiles_per_level[i],
            ],
            color=colors[i % len(colors)],
            linestyle="dashed",
        )
    plt.show()
# endregion

# region Show mean number of files in each level
# Avg number of files in each level
avg_files_in_lvl = np.mean(num_files_in_lvl_per_commit, axis=0)
# Hist the max number of files in each level
plt.bar(
    range(len(avg_files_in_lvl)),
    avg_files_in_lvl,
)
plt.show()
# endregion

# region === Analyze filetypes ===
import os

# Extract the directory from the new_path and old_path
df["new_folder"] = df["new_path"].apply(
    lambda x: os.path.dirname(x) if pd.notnull(x) else None
)
df["old_folder"] = df["old_path"].apply(
    lambda x: os.path.dirname(x) if pd.notnull(x) else None
)

from collections import defaultdict

# Initialize a defaultdict to store the counts
folder_counts = defaultdict(int)


# Define a function to update the counts based on the action
def update_counts(row):
    if row["action"] in ["add", "move"]:
        # Increment the count for the new folder
        key = (row["commit_id"], row["new_folder"], row["language"])
        folder_counts[key] += 1
    if row["action"] in ["delete", "move"]:
        # Decrement the count for the old folder
        key = (row["commit_id"], row["old_folder"], row["language"])
        folder_counts[key] -= 1


# Apply the function to each row
df.apply(update_counts, axis=1)

# Convert the defaultdict to a DataFrame
folder_counts_df = pd.DataFrame(
    [
        (commit_id, folder, language, count)
        for (commit_id, folder, language), count in folder_counts.items()
    ],
    columns=["commit_id", "folder", "language", "count"],
)
folder_counts_df.set_index(["commit_id", "folder", "language"], inplace=True)

# Print the counts for two commits
print(folder_counts_df.loc["c20b5602a3d58d1887f144aa12b0d95891ca1cea"])
print(folder_counts_df.loc["9f7c36531d6a23ab8b5bb3f456fb7a7c79370d65"])


# endregion
