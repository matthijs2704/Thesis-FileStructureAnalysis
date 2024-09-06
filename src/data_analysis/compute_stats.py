from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates

import pandas as pd
import matplotlib.pyplot as plt
import json
import argparse
from pathlib import Path

args = argparse.ArgumentParser(description="Process input project folder.")
args.add_argument("input_datafolder", type=Path, help="input data folder")
args = args.parse_args()
input_datafolder: Path = args.input_datafolder
data_path = input_datafolder / "data.jsonl"
commits_path = input_datafolder / "commits.jsonl"

# with open("output/commit_ids.json", "r") as json_file:
#     all_commit_ids = pd.read_json("output/commit_ids.json")
all_commit_ids = []

# read the JSONL file into a pandas DataFrame
df_commits = pd.read_json(commits_path, lines=True)
all_commits = df_commits["commit_id"].unique().tolist()

# read the JSONL file into a pandas DataFrame
df = pd.read_json(data_path, lines=True)

commits = df["commit_id"].unique().tolist()
# print(df.to_string())
print(
    f"{len(commits)} commit(s) out of {len(all_commit_ids)} with {df.size} structural changes"
)

# Compute the commit with the most structural changes
max_commit = df.groupby("commit_id").size().idxmax()
print(
    f"Commit with the most structural changes: {max_commit} ({df.groupby('commit_id').size().max()} changes)"
)

percentiles = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
actions = ["add", "delete", "move", "rename"]

# Compute statistics per commit_id for each action
stats = {}
for action in actions:
    action_stats = (
        df[df["action"] == action].groupby("commit_id").size().describe(percentiles)
    )
    stats[action] = {
        "count": action_stats["count"],
        "mean": action_stats["mean"],
        "std": action_stats["std"],
        "min": action_stats["min"],
        "25%": action_stats["25%"],
        "50%": action_stats["50%"],
        "75%": action_stats["75%"],
        "90%": action_stats["90%"],
        "95%": action_stats["95%"],
        "99%": action_stats["99%"],
        "max": action_stats["max"],
    }
    # Show the max commit for each action
    max_action_commit = df[df["action"] == action].groupby("commit_id").size().idxmax()
    print(
        f"Commit with the most {action} changes: {max_action_commit} ({df[df['action'] == action].groupby('commit_id').size().max()} changes)"
    )

# Compute statistics for total changes
changes_by_commit = df.groupby("commit_id").size()

total_stats = changes_by_commit.describe()
stats["total"] = {
    "count": total_stats["count"],
    "mean": total_stats["mean"],
    "std": total_stats["std"],
    "min": total_stats["min"],
    "25%": total_stats["25%"],
    "50%": total_stats["50%"],
    "75%": total_stats["75%"],
    "90%": action_stats["90%"],
    "95%": action_stats["95%"],
    "99%": action_stats["99%"],
    "max": total_stats["max"],
}

# Get the median number of changes
median_changes = stats["total"]["90%"]

# Get the number of commits with more structural changes than median
num_commits_above_median = changes_by_commit[changes_by_commit > median_changes].count()

# Print the number of commits with more structural changes than median
print(
    f"Number of commits with more structural changes than 90%: {num_commits_above_median}"
)

# Print the statistics in a tabular format
stats_df = pd.DataFrame(stats)
print(stats_df.to_string())

changes_by_commit = df.groupby(["commit_id", "action"]).size().unstack(fill_value=0)
changes_by_commit["total"] = changes_by_commit.sum(axis=1)
print(changes_by_commit.head())
# Create histogram for changes_by_commit
# plt.hist(changes_by_commit, bins=100)
# plt.xlabel("Number of changes")
# plt.ylabel("Number of commits")
# plt.title("Histogram of changes by commit")
# plt.show()

# BEGIN: 7jx9d3k4f8w1

# Merge the changes_by_commit DataFrame with the df_commits DataFrame to get the date of each commit
df_changes_by_commit = pd.merge(
    pd.DataFrame(changes_by_commit).reset_index(),
    df_commits,
    left_on="commit_id",
    right_on="commit_id",
)

# Convert the committed_date column to a datetime object
# df_changes_by_commit["commit_time"] = pd.to_datetime(
#     df_changes_by_commit["commit_time"]
# )

# Set the committed_date column as the index
df_changes_by_commit = df_changes_by_commit.set_index("commit_time")
df_changes_by_commit.sort_index(inplace=True)

# print(df_changes_by_commit.head(20))

# Get the commits with the most structural changes
interesting_commits = df_changes_by_commit[
    df_changes_by_commit["total"] > df_changes_by_commit["total"].quantile(0.95)
].sort_values(by="total", ascending=False)
print(f"Found {len(interesting_commits)} interesting commits")
interesting_commits["message"] = interesting_commits["message"].replace(
    to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True
)
print(interesting_commits[["commit_id", "total", "message"]])
exit(0)

# Create a line plot of changes_by_commit over time
fig, ax = plt.subplots(figsize=(12, 12))

# Exclude initial commit
df_changes_by_commit = df_changes_by_commit[1:]
ax.plot(
    df_changes_by_commit.index,
    df_changes_by_commit[actions],
)
date_form = DateFormatter("%m-%Y")
ax.xaxis.set_major_formatter(date_form)
# Ensure a major tick for each week using (interval=1)
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
ax.set(xlabel="Date", ylabel="Number of changes", title="Number of changes over time")

fig.legend(actions)
plt.show()
# END: 7jx9d3k4f8w1
