from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from json import loads
from typing import Iterable, List, Tuple

from pyrepositoryminer.pobjects import Blob, Object, Tree
from pyrepositoryminer.metrics.nativetree.main import NativeTreeMetric
from pyrepositoryminer.metrics.nativeblob.main import NativeBlobMetric, NativeBlobFilter
from pyrepositoryminer.metrics.diffblob.main import DiffBlobMetric
from pyrepositoryminer.metrics.structs import (
    NativeBlobMetricInput,
    Metric,
    ObjectIdentifier,
    NativeTreeMetricInput,
)
from utils.language_classifier import LanguageClassifier


# class Linguist(NativeBlobMetric):
#     filter = NativeBlobFilter(NativeBlobFilter.is_binary())

#     def __init__(self) -> None:
#         print("New instance of Linguist")
#         self.cache: set[str] = set()

#     lang_classifier = LanguageClassifier()

#     async def analyze(self, tup: NativeBlobMetricInput) -> Iterable[Metric]:
#         if tup.blob.name in self.cache:
#             return []


#         lang = self.lang_classifier.get_lang_by_blob(tup.blob.name, tup.blob.data)
#         self.cache.add(tup.blob.name)
#         return [
#             Metric(
#                 self.name,
#                 lang,
#                 False,
#                 ObjectIdentifier(tup.blob.id, tup.path),
#             )
#         ]
class Linguist(NativeTreeMetric):
    def __init__(self) -> None:
        # print("New instance of Linguist")
        self.cache: dict[str, str] = {}

    lang_classifier = LanguageClassifier()

    async def analyze(self, tup: NativeTreeMetricInput) -> Iterable[Metric]:
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

        return [Metric(self.name, new_data, False)]
        # return metrics
