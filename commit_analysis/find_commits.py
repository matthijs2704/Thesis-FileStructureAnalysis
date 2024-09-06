from typing import Optional, Iterable
from pygit2 import Repository, Commit
from pygit2 import GIT_SORT_REVERSE

from itertools import islice

from pyrepositoryminer.commands.utils.commits import (  # pylint: disable=import-outside-toplevel
    generate_walkers,
    iter_distinct,
)


def commit_has_structure_changes(commit: Commit) -> bool:
    num_parents = len(commit.parents)
    tree = commit.tree

    if num_parents == 1:
        diff = tree.diff_to_tree(commit.parents[0].tree, swap=True)
    elif num_parents > 1:
        diff = tree.diff_to_tree(commit.parents[0].tree, swap=True)
    else:
        diff = tree.diff_to_tree(swap=True)

    has_mutations = any(delta.status_char() in ["A", "D", "R"] for delta in diff.deltas)
    return has_mutations


def find_commits(
    repository: str,
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
            Repository(repository),
            branch_names,
            simplify_first_parent,
            sort if not sort_reverse else (sort | GIT_SORT_REVERSE),
        )
        for commit in walker
    )
    commit_ids = commit_ids if not drop_duplicates else iter_distinct(commit_ids)
    commit_ids = commit_ids if limit is None else islice(commit_ids, limit)
    return commit_ids
    for commit_id in commit_ids:
        echo(commit_id)
