from typing import Iterable, List, Tuple

# import sys

# sys.path.append("/Users/matthijs/Development/TUe/Thesis/ThesisCode")

from pyrepositoryminer.metrics.nativetree.main import NativeTreeMetric
from pyrepositoryminer.metrics.structs import Metric, NativeTreeMetricInput
from pyrepositoryminer.pobjects import Blob, Object, Tree
from pyrepositoryminer.metrics.nativeblob.main import NativeBlobFilter, NativeBlobMetric
from pygit2 import Diff, DiffDelta, Repository
from enum import Enum
from commit_analysis.utils.language_classifier import LanguageClassifier

# lang_classifier = LanguageClassifier()


class ModificationType(Enum):
    """
    Type of Modification. Can be ADD, COPY, RENAME, DELETE, MODIFY or UNKNOWN.
    """

    ADD = 1
    COPY = 2
    RENAME = 3
    DELETE = 4
    MODIFY = 5
    UNKNOWN = 6


class ExtractMutationsMetric(NativeTreeMetric):
    def is_rename(self, old_path: str, new_path: str):
        old_path = old_path.split("/")
        new_path = new_path.split("/")

        old_file_name = old_path.pop()
        new_file_name = new_path.pop()

        # if the file name is the same, it's not a rename, but a move
        return (
            "/".join(old_path) == "/".join(new_path) and old_file_name != new_file_name
        )

    async def analyze(self, tup: NativeTreeMetricInput) -> Iterable[Metric]:
        num_parents = len(tup.commit.parents)

        diff: Diff
        if num_parents == 1:
            diff = tup.tree.obj.diff_to_tree(tup.commit.parents[0].tree.obj, swap=True)
        elif num_parents > 1:
            diff = tup.tree.obj.diff_to_tree(tup.commit.parents[0].tree.obj, swap=True)
            # return [Metric(self.name, [], False)]
        else:
            diff = tup.tree.obj.diff_to_tree(swap=True)

        mut_chars = set(delta.status_char() for delta in diff.deltas)

        has_mutations = any(mut_char in mut_chars for mut_char in ["A", "D", "R"])

        if not has_mutations:
            return None

        # return []

        # parent_trees = tuple(parent.tree.obj for parent in tup.commit.parents)
        # diffs: list[Diff]
        # if not parent_trees:  # orphan commit is diffed to empty tree
        #     diffs = [tup.tree.obj.diff_to_tree(swap=True)]

        # else:
        #     diffs = [
        #         tup.tree.obj.diff_to_tree(parent_tree, swap=True)
        #         for parent_tree in parent_trees
        #     ]
        # q: List[Object] = [tup.tree]
        # lang_size: dict[str, int] = {}
        # while q:
        #     obj = q.pop(0)
        #     if isinstance(obj, Blob):
        #         lang = self.lang_classifier.get_lang_by_blob(obj.name, obj.data)
        #         lang_size.setdefault(lang, 0)
        #         lang_size[lang] += 1
        #     elif isinstance(obj, Tree):
        #         q.extend(list(obj))

        file_mutations = []
        # for diff in diffs:
        diff.find_similar()

        if any(nf_char in mut_chars for nf_char in ["A", "R"]):
            await self.analyzeFileNames(tup)

        for delta in diff.deltas:
            if delta.status_char() == "M":
                continue
            elif delta.status_char() == "A":
                file_mutations.append(
                    {
                        "action": "add",
                        "old_path": None,
                        "new_path": delta.new_file.path,
                        "language": self.cache.get(delta.new_file.path, None),
                    }
                )
            elif delta.status_char() == "D":
                file_mutations.append(
                    {
                        "action": "delete",
                        "old_path": delta.old_file.path,
                        "new_path": None,
                    }
                )
            elif delta.status_char() == "R" and self.is_rename(
                delta.old_file.path, delta.new_file.path
            ):
                file_mutations.append(
                    {
                        "action": "rename",
                        "old_path": delta.old_file.path,
                        "new_path": delta.new_file.path,
                        "language": self.cache.get(delta.new_file.path, None),
                    }
                )
            elif delta.status_char() == "R" and not self.is_rename(
                delta.old_file.path, delta.new_file.path
            ):
                file_mutations.append(
                    {
                        "action": "move",
                        "old_path": delta.old_file.path,
                        "new_path": delta.new_file.path,
                        "language": self.cache.get(delta.new_file.path, None),
                    }
                )

        metrics = [
            Metric(self.name, file_mutation, False) for file_mutation in file_mutations
        ]
        # metrics.append(Metric("linguist", analyzeFileNames, False))
        return metrics

    def __init__(self) -> None:
        # print("New instance of Linguist")
        self.cache: dict[str, str] = {}

    lang_classifier = LanguageClassifier()

    async def analyzeFileNames(self, tup: NativeTreeMetricInput) -> dict[str, str]:
        # Skip merge commits
        if len(tup.commit.parents) > 1:
            return None

        new_data: dict[str, str] = {}
        q: List[Tuple[Object, str]] = [(tup.tree, "")]
        while q:
            vo, path = q.pop(0)
            if isinstance(vo, Blob):
                blob_path = f"{path}/{vo.name}"

                if blob_path not in self.cache:
                    lang = self.lang_classifier.get_lang_by_blob(vo.name, vo.data)
                    self.cache[blob_path] = lang
                    new_data[blob_path] = lang
                else:
                    lang = self.cache[blob_path]
                    # print(f"Cache hit for {blob_path}")

                # yield Metric(self.name, new_data, False, ObjectIdentifier(vo.id, blob_path))

            elif isinstance(vo, Tree):
                p = f"{f'{path}/' if path else ''}{vo.name if vo.name else ''}"
                q.extend((sub_vo, p) for sub_vo in vo)

        if len(new_data) == 0:
            return None

        return new_data
