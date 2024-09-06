from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Iterator, List, Iterable

from pyrepositoryminer.commands.utils.metric import AvailableMetrics

from pyrepositoryminer.commands.analyze import (  # pylint: disable=import-outside-toplevel
    single_worker_Pool,
    # make_pool,
)

from multiprocessing import Pool  # pylint: disable=import-outside-toplevel
from commit_analysis.custom_analyze import (  # pylint: disable=import-outside-toplevel
    worker,
    CustomCommitOutput,
    InitArgs,
    initialize,
)
from pyrepositoryminer.commands.utils.metric import (  # pylint: disable=import-outside-toplevel
    import_metric,
)
from pyrepositoryminer.metrics import (  # pylint: disable=import-outside-toplevel
    all_metrics,
)

if TYPE_CHECKING:
    from multiprocessing.pool import Pool as tcpool


@contextmanager
def make_pool(
    workers: int,
    repository: Path,
    metrics: List[AvailableMetrics],
    custom_metrics: List[str],
) -> Iterator["tcpool"]:
    if workers <= 1:
        initialize(
            InitArgs(
                repository,
                tuple({metric.value for metric in metrics} & all_metrics.keys()),
                tuple(map(import_metric, set(custom_metrics))),
            )
        )
        with single_worker_Pool() as pool:
            yield pool
        return

    with Pool(
        max(workers, 1),
        initialize,
        (
            InitArgs(
                repository,
                tuple({metric.value for metric in metrics} & all_metrics.keys()),
                tuple(map(import_metric, set(custom_metrics))),
            ),
        ),
    ) as pool:
        yield pool


def analyzeRepo(
    repository: str,
    metrics: List[AvailableMetrics],
    commits: Iterable[str],
    custom_metrics: List[str] = [],
    workers: int = 1,
) -> Generator[CustomCommitOutput, None, None]:
    """Analyze commits of a repository."""

    metrics = metrics if metrics else []
    ids = (
        id.strip() for id in commits  # pylint: disable=superfluous-parens
    )  # print(len(ids))
    with make_pool(workers, repository, metrics, custom_metrics) as pool:
        results = (res for res in pool.imap(worker, ids) if res is not None)
        yield from results
