from functools import partial
import json
from pathlib import Path

from repository_analysis.custom_pyrepositoryminer_analyze import CustomCommitOutput


class CommitInfo:
    def __init__(
        self, commit_id: str, commit_time: int, message: str, parents: list[str]
    ):
        self.commit_id = commit_id
        self.commit_time = commit_time
        self.message = message
        self.parents = parents

    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__)


class FileStructureMutation:
    def __init__(self, commit_id, action, old_path, new_path, language):
        self.commit_id = commit_id
        self.action = action
        self.old_path = old_path
        self.new_path = new_path
        self.language = language

    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__)


def convert_pyrepositoryminer_output(
    data: CustomCommitOutput,
) -> (CommitInfo, list[FileStructureMutation]):
    # print(data)
    commit_id = data["id"]
    commit_info = CommitInfo(
        commit_id,
        data["commit_time"],
        data["message"],
        data.get("parent_ids", []),
    )

    file_mutations = data["output"]["extractmutationsmetric"]
    out_mutations = list(map(partial(convert_mutation, commit_id), file_mutations))
    return commit_info, out_mutations


def convert_mutation(commit_id: str, input: dict) -> FileStructureMutation:
    output = FileStructureMutation(
        commit_id,
        input["action"],
        input["old_path"],
        input["new_path"],
        input.get("language", None),
    )
    return output
