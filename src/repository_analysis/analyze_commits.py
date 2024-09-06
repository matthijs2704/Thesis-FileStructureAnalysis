from typing import Iterable, Iterator
from repository_analysis.analyze_repo import analyzeRepo
from repository_analysis.custom_pyrepositoryminer_analyze import CustomCommitOutput


def analyze_commits(
    repo: str, commit_ids: Iterable[str], no_workers: int = 1
) -> Iterator[CustomCommitOutput]:
    result = analyzeRepo(
        repo,
        None,
        commit_ids,
        ["repository_analysis:ExtractMutationsMetric"],
        no_workers,
    )

    for item in result:
        if len(item["output"].keys()) == 0:
            continue

        yield item
