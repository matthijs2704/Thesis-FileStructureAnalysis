import pandas as pd
from pathlib import Path
from data_analysis.preprocess.file_tree_depth import (
    compute_file_depth,
    get_max_file_depth,
)
import json
import plotly.express as px

project_directory_base = Path("output/")

df_github = pd.read_csv(f"datasets/github_data_filtered.csv", keep_default_na=False)
df_github = df_github[~df_github["ignored"]]
top_10_projects = (
    df_github.groupby("lang")["stargazers_count"].nlargest(10).reset_index()
)
df_github = df_github[df_github.index.isin(top_10_projects["level_1"].unique())]


def get_repo_info(project_id):
    project_directory = project_directory_base / project_id
    with open(project_directory / "repo_info.json", "r") as f:
        repo_info = json.load(f)
    return repo_info


repo_infos = {}

for idx, project in df_github.iterrows():
    project_name = project["owner.name"] + "/" + project["name"]
    project_id = project["owner.name"].lower() + "_" + project["name"].lower()

    repo_info = get_repo_info(project_id)
    repo_infos[project_id] = repo_info

df = pd.DataFrame.from_dict(repo_infos, orient="index")
print(df)

print(df[df["no_contributors"] > 6000])
print(df[df["no_commits"] > 60000])
wait = input("Press enter to continue")

# Show distribution of no_contributors
print(df["no_contributors"].describe())
px.histogram(df, x="no_contributors", nbins=100, hover_data=df.index.name).show()

# Show distribution of no_commits
print(df["no_commits"].describe())
px.histogram(df, x="no_commits", nbins=100, hover_data=df.index.name).show()

# Show distribution of no_struc_changing_commits
print(df["no_struc_changing_commits"].describe())
px.histogram(
    df, x="no_struc_changing_commits", nbins=100, hover_data=df.index.name
).show()

# Show percentage of commits that are structural changing
perc = df["no_struc_changing_commits"] / df["no_commits"] * 100
print(df[perc > 70])
print(perc.describe())
px.histogram(df, x=perc, nbins=50, hover_data=df.index.name).show()
