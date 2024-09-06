import pygit2 as git
from pygit2.repository import Repository, Commit
from pathlib import Path
import hashlib


def count_contributors(repoPath: Path, branchStr: str) -> int:
    repo = Repository(repoPath)
    contributors = set()
    commit: Commit

    branch = repo.lookup_branch(branchStr)
    walker = repo.walk(branch.target, git.GIT_SORT_NONE)
    for commit in walker:
        if len(commit.parents) > 1:
            continue

        author = commit.author
        email_hash = hashlib.sha256(author.raw_email).hexdigest()
        contributors.add(email_hash)
    return len(contributors)
