from typing import Iterable

from pyrepositoryminer.metrics.nativetree.main import NativeTreeMetric
from pyrepositoryminer.metrics.structs import Metric, NativeTreeMetricInput
from pygit2 import Diff


class HasMutationsMetric(NativeTreeMetric):
    async def analyze(self, tup: NativeTreeMetricInput) -> Iterable[Metric] | None:
        num_parents = len(tup.commit.parents)

        diff: Diff
        if num_parents == 1:
            diff = tup.tree.obj.diff_to_tree(tup.commit.parents[0].tree.obj, swap=True)
        elif num_parents > 1:
            diff = tup.tree.obj.diff_to_tree(tup.commit.parents[0].tree.obj, swap=True)
            # return [Metric(self.name, [], False)]
        else:
            diff = tup.tree.obj.diff_to_tree(swap=True)

        has_mutations = any(
            delta.status_char() in ["A", "D", "R"] for delta in diff.deltas
        )
        if has_mutations:
            return [
                Metric(self.name, has_mutations, False),
            ]
        return None
