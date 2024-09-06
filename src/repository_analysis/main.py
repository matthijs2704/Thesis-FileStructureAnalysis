from typing import Iterable, Iterator, Tuple
from repository_analysis import (
    find_branches,
    find_commits_with_time,
    analyze_commits,
    extract_repo_info,
    convert_pyrepositoryminer_output,
    CommitInfo,
    FileStructureMutation,
)

from data_analysis.preprocess import preprocess
import pandas as pd
import numpy as np

import argparse
from pathlib import Path
import csv
import json

from pygit2 import GIT_SORT_NONE, GIT_SORT_TIME, GIT_SORT_TOPOLOGICAL
from typer import echo
import time

parser = argparse.ArgumentParser(description="Process input and output filenames.")
parser.add_argument("repo_path", type=Path, help="repository path")
parser.add_argument("output_path", type=Path, help="output path")
parser.add_argument("--workers", type=int, help="workers", default=1, required=False)


def find_commit_ids(
    git_repo_path: Path, branches: Iterable[str]
) -> Iterator[Tuple[str, int]]:
    # Find the commit ids for the full git history
    yield from find_commits_with_time(
        git_repo_path,
        branches,
        sort=GIT_SORT_TOPOLOGICAL | GIT_SORT_TIME,
        sort_reverse=True,
        simplify_first_parent=True,
        drop_duplicates=True,
        limit=None,
    )


commit_data_dtypes = {
    "commit_id": pd.StringDtype(),
    "commit_time": np.int64,
    "message": pd.StringDtype(),
    "parents": pd.StringDtype(),
}

mutations_data_dtypes = {
    "commit_id": pd.StringDtype(),
    "action": pd.StringDtype(),
    "old_path": pd.StringDtype(),
    "new_path": pd.StringDtype(),
    "language": pd.StringDtype(),
}

if __name__ == "__main__":
    args = parser.parse_args()

    repo_path: Path = args.repo_path
    repo_name = repo_path.name

    output_path: Path = args.output_path

    # output_path = project_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    # Find the main branch of the repo
    branches = list(find_branches(repo_path))
    main_branch = branches[0]

    # Find all the commit ids, with time, and save them to a file
    all_commits_file_path = output_path / "all_commits.parquet"
    all_commits = list(find_commit_ids(repo_path, [main_branch]))
    all_commits_df = pd.DataFrame(all_commits, columns=["commit_id", "commit_time"])
    all_commits_df = all_commits_df.reset_index().rename(
        columns={"index": "commit_index"}
    )
    all_commits_df.to_parquet(all_commits_file_path, index=False)
    all_commits_df = all_commits_df.set_index("commit_id")

    commit_ids = [commit_id for commit_id, _ in all_commits]

    # Do the commit analysis
    echo("Starting commit analysis")
    start_time = time.time()
    commits_data: list[dict] = []
    mutations_data: list[dict] = []
    struc_commit_ids = []
    for commit_output in analyze_commits(repo_path, commit_ids, args.workers):
        commit_info: CommitInfo
        out_mutations: list[FileStructureMutation]
        commit_info, out_mutations = convert_pyrepositoryminer_output(commit_output)
        struc_commit_ids.append(commit_info.commit_id)
        commit_index = all_commits_df.loc[commit_info.commit_id]["commit_index"]
        commits_data.append(
            {
                "commit_id": commit_info.commit_id,
                "commit_index": commit_index,
                "commit_time": commit_info.commit_time,
                "message": commit_info.message,
                "parents": ",".join(commit_info.parents),
            }
        )
        mutations_data.extend([mut.__dict__ for mut in out_mutations])

    duration = time.time() - start_time
    echo(f"Commit analysis complete in {str(duration)} seconds")

    print("Creating DataFrames")
    commits_df = pd.DataFrame(commits_data).astype(commit_data_dtypes)
    mutations_df = pd.DataFrame(mutations_data).astype(mutations_data_dtypes)

    # Set the commit_id as the index for the commits DataFrame
    commits_df = commits_df.set_index("commit_id")

    # Sort the commits DataFrame by commit_index
    commits_df = commits_df.sort_values("commit_index", ascending=True)

    # Sort the mutations DataFrame by order of commit_id in commits DataFrame
    mutations_df = mutations_df.merge(commits_df[["commit_index"]], on="commit_id")
    mutations_df = mutations_df.sort_values("commit_index", ascending=True)
    mutations_df = mutations_df.drop(columns="commit_index")

    # Preprocess data for analysis
    echo("Preprocessing data for analysis")
    commits_df, mutations_df, max_file_depth = preprocess(commits_df, mutations_df)
    echo("Preprocessing done!")

    # Save the data as parquet
    # This saves two files: commits.parquet and mutations.parquet
    echo("Saving datasets")
    commits_df.to_parquet(output_path / "commits.parquet")
    mutations_df.to_parquet(output_path / "mutations.parquet")

    echo("Saving repository info")
    repo_stats = extract_repo_info(repo_path, main_branch, commit_ids, struc_commit_ids)
    repo_stats["max_file_depth"] = int(max_file_depth)
    with open(output_path / "repo_info.json", "w") as f:
        json.dump(repo_stats, f)
    echo("Repository info saved!")

    echo("Finished extraction!")
