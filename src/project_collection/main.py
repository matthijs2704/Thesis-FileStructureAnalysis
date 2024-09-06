from typing import Iterator
from repository_analysis import (
    find_branches,
    find_commits,
)

import argparse
from pathlib import Path

import pygit2
from pygit2 import clone_repository, GIT_SORT_NONE
from pygit2.remotes import TransferProgress
from typing import Optional
from typer import echo
import time

import shutil

parser = argparse.ArgumentParser(description="Process input and output directory.")
parser.add_argument("repository_url", type=str, help="repo url")
parser.add_argument("tmp_local_dir", type=Path, help="directory to clone repo to")
parser.add_argument("out_local_dir", type=Path, help="directory to copy final repo to")


class GitCloneCallbacks(pygit2.RemoteCallbacks):
    prev_perc = 0

    def __init__(self):
        super().__init__()
        echo("Cloning", nl=False)

    def transfer_progress(self, stats: TransferProgress):
        perc = stats.indexed_objects / stats.total_objects * 10
        rounded_perc = round(perc) * 10
        if rounded_perc != self.prev_perc:
            self.prev_perc = rounded_perc
            echo(f"..{rounded_perc}%", nl=False)
        if perc == 10:
            echo("")


if __name__ == "__main__":
    args = parser.parse_args()
    repo_url: str = args.repository_url
    tmp_local_dir: Path = args.tmp_local_dir
    out_local_dir: Path = args.out_local_dir

    # Check if repo is already cloned
    if out_local_dir.exists():
        echo(f"Repo already exists in {out_local_dir}, skipping!")
        exit(0)

    # Check if repo is already cloned
    if tmp_local_dir.exists():
        shutil.rmtree(tmp_local_dir)

    # If the repo is not cloned, clone it to tmp disk
    echo(f"Starting clone of repo: {repo_url}")
    start_time = time.time()
    clone_repository(
        repo_url,
        tmp_local_dir,
        bare=True,
        callbacks=GitCloneCallbacks(),
    )
    elapsed = time.time() - start_time
    echo(f"Cloning done in {elapsed:.2f}")

    # Copy repo to final output directory
    echo(f"Copying repo to {out_local_dir}")
    shutil.move(tmp_local_dir, out_local_dir)

    elapsed = time.time() - start_time
    echo(f"Clone complete in {elapsed:.2f}")
    exit(0)
