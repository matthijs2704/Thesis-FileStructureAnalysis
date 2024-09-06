from typing import Iterable
from pygit2 import Repository
from pygit2.repository import Branches
from pathlib import Path


def find_branches(
    repository: Path, local: bool = True, remote: bool = False
) -> Branches:
    repo = Repository(repository)
    branches: Iterable[str]
    if local and remote:
        branches = repo.branches
    elif local:
        branches = repo.branches.local
    elif remote:
        branches = repo.branches.remote
    return branches
