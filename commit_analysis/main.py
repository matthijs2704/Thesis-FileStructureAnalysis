from typing import Generator
from typer import echo  # pylint: disable=import-outside-toplevel
from pygit2 import (
    GIT_SORT_NONE,
    GIT_SORT_REVERSE,
    GIT_SORT_TIME,
    GIT_SORT_TOPOLOGICAL,
)

import sys

from commit_analysis.find_branches import find_branches
from commit_analysis.find_commits import find_commits
from commit_analysis.analyze_repo import analyzeRepo
from commit_analysis.custom_analyze import CustomCommitOutput

import json

simplify_first_parent = True
sort_reverse = True
sort = GIT_SORT_TOPOLOGICAL
drop_duplicates = True
limit = None
workers = 8


def do_commit_analysis(repo: str) -> Generator[CustomCommitOutput, None, None]:
    branches = find_branches(repo)
    commit_ids = list(
        find_commits(
            repo,
            branches,
            sort,
            sort_reverse,
            simplify_first_parent,
            drop_duplicates,
            limit,
        )
    )

    with open("output/commit_ids.json", "w") as f:
        json.dump(commit_ids, f)

    result = analyzeRepo(
        repo,
        None,
        commit_ids,
        ["commit_analysis:ExtractMutationsMetric"],
        workers,
    )

    for item in result:
        if len(item["output"].keys()) == 0:
            continue

        yield item
        # echo(json.dumps(item))
        # file_mutations_per_commit[id] = output
        # f.write(json.dumps(item) + "\n")
