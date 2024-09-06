from typing import Iterable
from pygit2 import Repository


def find_branches(repository: str, local: bool = True, remote: bool = False) -> str:
    repo = Repository(repository)
    branches: Iterable[str]
    if local and remote:
        branches = repo.branches
    elif local:
        branches = repo.branches.local
    elif remote:
        branches = repo.branches.remote
    return branches
