from typing import Optional, Iterable, Iterator, Tuple
from pygit2 import Repository
from pygit2 import GIT_SORT_REVERSE

from itertools import islice
from pathlib import Path

from pyrepositoryminer.commands.utils.commits import (  # pylint: disable=import-outside-toplevel
    generate_walkers,
    iter_distinct,
)


def find_commits_with_time(
    repository_path: Path,
    branch_names: Iterable[str],
    sort: int,
    sort_reverse: bool,
    simplify_first_parent: bool,
    drop_duplicates: bool,
    limit: Optional[int],
):
    commit_ids: Iterator[Tuple[str, int]] = (
        (str(commit.id), commit.commit_time)
        for walker in generate_walkers(
            Repository(repository_path),
            branch_names,
            simplify_first_parent,
            sort if not sort_reverse else (sort | GIT_SORT_REVERSE),
        )
        for commit in walker
    )
    commit_ids = commit_ids if not drop_duplicates else iter_distinct(commit_ids)
    commit_ids = commit_ids if limit is None else islice(commit_ids, limit)
    return commit_ids


def find_commits(
    repository_path: Path,
    branch_names: Iterable[str],
    sort: int,
    sort_reverse: bool,
    simplify_first_parent: bool,
    drop_duplicates: bool,
    limit: Optional[int],
):
    commit_ids: Iterable[str] = (
        str(commit.id)
        for walker in generate_walkers(
            Repository(repository_path),
            branch_names,
            simplify_first_parent,
            sort if not sort_reverse else (sort | GIT_SORT_REVERSE),
        )
        for commit in walker
    )
    commit_ids = commit_ids if not drop_duplicates else iter_distinct(commit_ids)
    commit_ids = commit_ids if limit is None else islice(commit_ids, limit)
    return commit_ids
