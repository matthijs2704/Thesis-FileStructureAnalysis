from typing import Iterable, List, Tuple

# import sys

# sys.path.append("/Users/matthijs/Development/TUe/Thesis/ThesisCode")

from pyrepositoryminer.metrics.nativetree.main import NativeTreeMetric
from pyrepositoryminer.metrics.structs import Metric, NativeTreeMetricInput
from pyrepositoryminer.pobjects import Blob, Object, Tree
from pygit2 import Diff, DiffFile
from enum import Enum
from repository_analysis.utils.language_classifier import LanguageClassifier

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

    def get_file_path(self, file: DiffFile) -> str:
        try:
            return file.path
        except:
            import chardet

            cdout = chardet.detect(file.raw_path)
            return file.raw_path.decode(cdout["encoding"])

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

        has_mutations = any(d.status_char() in ["A", "D", "R"] for d in diff.deltas)

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

        # if any(nf_char in mut_chars for nf_char in ["A", "R"]):
        #     await self.analyzeFileNames(tup)

        for patch in diff:
            delta = patch.delta

            if delta.status_char() == "M":
                continue

            # Naive approach to get file content from patch
            patch_content = "".join(
                [line.content for hunk in patch.hunks for line in hunk.lines],
            ).encode()

            if delta.status_char() == "A":
                new_file_path = self.get_file_path(delta.new_file)
                file_mutations.append(
                    {
                        "action": "add",
                        "old_path": None,
                        "new_path": new_file_path,
                        "language": self.get_lang_by_blob(new_file_path, patch_content),
                    }
                )
            elif delta.status_char() == "D":
                old_file_path = self.get_file_path(delta.old_file)
                file_mutations.append(
                    {
                        "action": "delete",
                        "old_path": old_file_path,
                        "new_path": None,
                        "language": self.cache.pop(old_file_path, None),
                    }
                )
            elif delta.status_char() == "R":
                old_file_path = self.get_file_path(delta.old_file)
                new_file_path = self.get_file_path(delta.new_file)
                is_rename = self.is_rename(old_file_path, new_file_path)
                old_lang = self.cache.pop(old_file_path, None)
                new_lang = self.get_lang_by_blob(new_file_path, patch_content)
                if old_lang and not new_lang:
                    self.cache[new_file_path] = new_lang = old_lang
                file_mutations.append(
                    {
                        "action": "rename" if is_rename else "move",
                        "old_path": old_file_path,
                        "new_path": new_file_path,
                        "language": new_lang,
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

    def get_lang_by_blob(self, blob_path: str, blob_data: bytes):
        if blob_path not in self.cache or self.cache[blob_path] is None:
            lang = self.lang_classifier.get_lang_by_blob(blob_path, blob_data)
            self.cache[blob_path] = lang
            return lang
        else:
            return self.cache[blob_path]

    async def analyzeFileNames(self, tup: NativeTreeMetricInput) -> dict[str, str]:
        # Skip merge commits
        # if len(tup.commit.parents) > 1:
        #     return None

        new_data: dict[str, str] = {}
        q: List[Tuple[Object, str]] = [(tup.tree, "")]
        while q:
            vo, path = q.pop(0)

            try:
                vo_name = vo.name
            except:
                import chardet

                cdout = chardet.detect(vo.obj.raw_name)
                vo_name = vo.obj.raw_name.decode(cdout["encoding"])

            if isinstance(vo, Blob):
                blob_path = f"{path}/{vo_name}"
                lang = self.get_lang_by_blob(blob_path, vo.data)
                new_data[blob_path] = lang
                # yield Metric(self.name, new_data, False, ObjectIdentifier(vo.id, blob_path))

            elif isinstance(vo, Tree):
                p = f"{f'{path}/' if path else ''}{vo_name if vo_name else ''}"
                q.extend((sub_vo, p) for sub_vo in vo)

        if len(new_data) == 0:
            return None

        return new_data
