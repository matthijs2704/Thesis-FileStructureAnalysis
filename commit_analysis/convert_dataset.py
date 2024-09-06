import json
from pathlib import Path


def convert_dataset(input_file: Path, output_dir: Path):
    if not output_dir.exists() or not output_dir.is_dir():
        return

    with open(input_file, "r") as json_file:
        json_list = list(json_file)

    with open(output_dir.joinpath("commits.jsonl"), "w") as commits_output_file, open(
        output_dir.joinpath("data.jsonl"), "w"
    ) as output_file:
        for json_str in json_list:
            data = json.loads(json_str)
            # print(data["output"])
            commit_info = {
                "commit_id": data["id"],
                "commit_time": data["commit_time"],
                "committer": data["committer"],
                "author": data["author"],
                "message": data["message"],
            }
            commits_output_file.write(json.dumps(commit_info) + "\n")

            file_mutations = data["output"]["extractmutationsmetric"]
            for fileMod in file_mutations:
                output = {}
                output["commit_id"] = data["id"]
                output["action"] = fileMod.get("action", None)
                output["old_path"] = fileMod.get("old_path", None)
                output["new_path"] = fileMod.get("new_path", None)
                output["language"] = fileMod.get("language", None)
                output_file.write(json.dumps(output) + "\n")
