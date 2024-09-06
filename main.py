from commit_analysis import do_commit_analysis, convert_dataset
from commit_classification import classify_commits

import argparse
from pathlib import Path
import json
import os

from pyrepositoryminer.commands.clone import clone

parser = argparse.ArgumentParser(description="Process input and output filenames.")
parser.add_argument("repository", type=Path, help="input filename")
# parser.add_argument("output_filename", type=str, help="output filename")

if __name__ == "__main__":
    args = parser.parse_args()
    repo_path: Path = args.repository
    repo_name = repo_path.name.removesuffix(".git")

    # If the repo is not cloned, clone it to the RAM disk
    if not os.path.exists(f"/Volumes/RAMDisk/{repo_name}.git"):
        clone(repo_path, f"/Volumes/RAMDisk/{repo_name}.git")

    # Do the commit analysis
    if os.path.exists("output/raw_data.jsonl"):
        os.remove("output/raw_data.jsonl")

    with open("output/raw_data.jsonl", "a") as output_file:
        for commit_output in do_commit_analysis(args.repository):
            output_file.write(json.dumps(commit_output) + "\n")

    # Convert the dataset to a more usable format
    # This creates two files:
    # - output/commits.jsonl (contains commit metadata)
    # - output/data.jsonl (contains commit mutations)
    convert_dataset(Path("output/raw_data.jsonl"), output_dir=Path("output"))

    # Classify the commit messages
    classify_commits(
        Path("output/commits.jsonl"),
        output_file=Path("output/commits_classified.jsonl"),
    )

    # TODO: Do automated data analysis and statistics extraction
