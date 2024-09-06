"""Analyze a commit w.r.t tree-, blob- or unit-metrics.

Global variables are accessed in the context of a worker.
"""
from asyncio import run
from asyncio.tasks import as_completed
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypedDict,
)

from pygit2 import Commit, Repository
from uvloop import install

from pyrepositoryminer.metrics import all_metrics
from pyrepositoryminer.metrics.diffblob.main import DiffBlobMetric, DiffBlobVisitor
from pyrepositoryminer.metrics.diffdir.main import DiffDirMetric, DiffDirVisitor
from pyrepositoryminer.metrics.dir.main import DirMetric, DirVisitor
from pyrepositoryminer.metrics.nativeblob.main import (
    NativeBlobMetric,
    NativeBlobVisitor,
)
from pyrepositoryminer.metrics.nativetree.main import (
    NativeTreeMetric,
    NativeTreeVisitor,
)
from pyrepositoryminer.metrics.structs import Metric
from pyrepositoryminer.output import SignatureOutput, parse_signature, parse_commit
from pyrepositoryminer.pobjects import Object


class InitArgs(NamedTuple):
    repository: Path
    metrics: Tuple[str, ...]
    custom_metrics: Tuple[Any, ...]  # TODO make this a metric abc


# pylint: disable=global-statement
repo: Repository
native_blob_metrics: Tuple[Any, ...]
native_blob_visitor: NativeBlobVisitor
diff_blob_metrics: Tuple[Any, ...]
diff_blob_visitor: DiffBlobVisitor
native_tree_metrics: Tuple[Any, ...]
native_tree_visitor: NativeTreeVisitor
dir_metrics: Tuple[Any, ...]
dir_visitor: DirVisitor
diffdir_visitor: DiffDirVisitor
diffdir_metrics: Tuple[Any, ...]


# async def categorize_metrics(
#     *futures: Awaitable[Iterable[Metric]],
# ) -> Tuple[Iterable[Metric], Iterable[Metric], Iterable[Metric]]:
#     toplevel = []
#     objectlevel = []
#     subobjectlevel = []
#     for future in as_completed(futures):
#         for metric in await future:
#             if metric.object is None:
#                 toplevel.append(metric)
#             elif metric.subobject is None:
#                 objectlevel.append(metric)
#             else:
#                 subobjectlevel.append(metric)
#     return toplevel, objectlevel, subobjectlevel


class CustomCommitOutput(TypedDict):
    id: str
    author: SignatureOutput
    commit_time: int
    commit_time_offset: int
    committer: SignatureOutput
    message: str
    parent_ids: List[str]
    output: List[Any]


async def custom_analyze(commit_id: str) -> Optional[CustomCommitOutput]:
    try:
        commit = repo.get(commit_id)
    except ValueError:
        return None
    else:
        if commit is None or not isinstance(commit, Commit):
            return None
    root = Object.from_pobject(commit)
    futures: List[Awaitable[Iterable[Metric]]] = []
    if native_blob_metrics:
        futures.extend(
            m(blob_tup)
            for blob_tup in native_blob_visitor(root)
            for m in native_blob_metrics
            if not m.filter(blob_tup)
        )
    if diff_blob_metrics:
        futures.extend(
            m(blob_tup)
            for blob_tup in diff_blob_visitor(root)
            for m in diff_blob_metrics
            if not m.filter(blob_tup)
        )
    if native_tree_metrics:
        tree_tup = native_tree_visitor(root)
        futures.extend(m(tree_tup) for m in native_tree_metrics)
    if dir_metrics:
        dir_tup = dir_visitor(root)
        futures.extend(m(dir_tup) for m in dir_metrics)
    if diffdir_metrics:
        diffdir_tup = diffdir_visitor(root)
        futures.extend(m(diffdir_tup) for m in diffdir_metrics)

    output = {}
    for future in as_completed(futures):
        res: Iterable[Metric] | None = await future
        if res is None:
            continue
            # if isinstance(res, list):
            #     output.extend(map(lambda res: res.value, res))
            # else:
        for metric in res:
            output.setdefault(metric.name, []).append(metric.value)

    if dir_metrics:
        dir_visitor.close()
    if diffdir_metrics:
        diffdir_visitor.close()

    return CustomCommitOutput(
        id=str(commit.id),
        author=parse_signature(commit.author),
        commit_time=int(commit.commit_time),
        commit_time_offset=int(commit.commit_time_offset),
        committer=parse_signature(commit.committer),
        message=str(commit.message),
        parent_ids=[str(id) for id in commit.parent_ids],
        output=output,
    )


def initialize(init_args: InitArgs) -> None:
    install()
    global repo
    global native_blob_metrics, native_blob_visitor
    global diff_blob_metrics, diff_blob_visitor
    global native_tree_metrics, native_tree_visitor
    global dir_metrics, dir_visitor
    global diffdir_metrics, diffdir_visitor

    def get_metrics(superclass) -> Tuple:  # type: ignore
        return tuple(
            [
                all_metrics[m]()
                for m in init_args.metrics
                if issubclass(all_metrics[m], superclass)
            ]
            + [m() for m in init_args.custom_metrics if issubclass(m, superclass)]
        )

    repo = Repository(init_args.repository)
    native_blob_metrics = get_metrics(NativeBlobMetric)
    native_blob_visitor = NativeBlobVisitor()
    diff_blob_metrics = get_metrics(DiffBlobMetric)
    diff_blob_visitor = DiffBlobVisitor(repo)
    native_tree_metrics = get_metrics(NativeTreeMetric)
    native_tree_visitor = NativeTreeVisitor()
    dir_metrics = get_metrics(DirMetric)
    dir_visitor = DirVisitor(repo)
    diffdir_metrics = get_metrics(DiffDirMetric)
    diffdir_visitor = DiffDirVisitor(repo)


def worker(commit_id: str) -> Optional[CustomCommitOutput]:
    output = run(custom_analyze(commit_id))
    if output is None:
        return None
    return output
