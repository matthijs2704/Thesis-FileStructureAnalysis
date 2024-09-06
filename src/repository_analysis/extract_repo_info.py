from repository_analysis import count_contributors
from pathlib import Path


def extract_repo_info(
    repo_path: Path, branch: str, all_commit_ids: list[str], struc_commit_ids: list[str]
) -> dict:
    repo_stats = {}
    repo_stats["first_commit"] = all_commit_ids[0]
    repo_stats["last_commit"] = all_commit_ids[-1]

    # First step: aggregate and remove authors, committers from the commits file
    # Calculate the number of contributors
    repo_stats["no_contributors"] = count_contributors(repo_path, branch)

    # Calculate the number of analysed commits
    repo_stats["no_commits"] = len(all_commit_ids)

    # Calculate the number of commits
    repo_stats["no_struc_changing_commits"] = len(struc_commit_ids)

    return repo_stats
