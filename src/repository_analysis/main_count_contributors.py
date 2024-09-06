from repository_analysis import count_contributors, find_branches
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description="Process input repo.")
parser.add_argument("repo_path", type=Path, help="repository path")

if __name__ == "__main__":
    args = parser.parse_args()

    repo_path: Path = args.repo_path
    repo_name = repo_path.name

    branches = list(find_branches(repo_path))
    branch = branches[0]
    num_contr = count_contributors(repo_path, branch)
    print(num_contr)
