import argparse
import numpy
import pandas as pd

parser = argparse.ArgumentParser(description="Process input and output filenames.")
parser.add_argument("input_filename", type=str, help="input filename")
# parser.add_argument("output_filename", type=str, help="output filename")
args = parser.parse_args()

# df = pd.read_json(path_or_buf=args.input_filename, lines=True)
# df = df.explode('metrics')
# # Extract the "value" column from the "metrics" column
# df['value'] = df['metrics'].apply(lambda x: x['value'] if 'value' in x else None if x is not numpy.NaN else None)

# # Explode the "value" column
# df = df.explode('value')

# df = df.dropna(subset=['value'])

# # Extract the "action", "old_path", and "new_path" columns from the "value" column
# df['action'] = df['value'].apply(lambda x: x['action'] if 'action' in x else None if x is not numpy.NaN else None)
# df['old_path'] = df['value'].apply(lambda x: x['old_path'] if 'old_path' in x else None if x is not numpy.NaN else None)
# df['new_path'] = df['value'].apply(lambda x: x['new_path'] if 'new_path' in x else None if x is not numpy.NaN else None)

# df['committer'] = df['committer'].apply(lambda x: f"{x['name']} ({x['email']})" if 'name' in x else None if x is not numpy.NaN else None)
# df['author'] = df['author'].apply(lambda x: f"{x['name']} ({x['email']})" if 'name' in x else None if x is not numpy.NaN else None)

# df = df.drop(columns=['commit_time_offset','metrics', 'value','objects','parent_ids'])

# # print (df.columns)
# df.to_json(path_or_buf=args.output_filename, orient="records", lines=True)
import json

with open(args.input_filename, "r") as json_file:
    json_list = list(json_file)

# with open("output/data.jsonl", "w") as output_file, open(
#     "output/language_data.jsonl", "w"
# ) as lang_output_file:
#     for json_str in json_list:
#         data = json.loads(json_str)
#         for metric in data["output"]["ExtractMutationsMetric"]:
#             if (
#                 "name" in metric
#                 and metric["name"] == "ExtractMutationsMetric"
#                 and "value" in metric
#             ):
#                 value = metric["value"]
#                 if value is None:
#                     continue
#                 for fileMod in value:
#                     output = {}
#                     output["commit_id"] = data["id"]
#                     output["commit_time"] = data["commit_time"]
#                     output[
#                         "committer"
#                     ] = f"{data['committer']['name']} ({data['committer']['email']})"
#                     output[
#                         "author"
#                     ] = f"{data['author']['name']} ({data['author']['email']})"
#                     output["action"] = fileMod.get("action", None)
#                     output["old_path"] = fileMod.get("old_path", None)
#                     output["new_path"] = fileMod.get("new_path", None)
#                     output_file.write(json.dumps(output) + "\n")

#             elif (
#                 "name" in metric and metric["name"] == "linguist" and "value" in metric
#             ):
#                 value = metric["value"]
#                 if value is None:
#                     continue
#                 lang_output_file.write(json.dumps(value) + "\n")

with open("output/commits.jsonl", "w") as commits_output_file, open(
    "output/data.jsonl", "w"
) as output_file, open("output/language_data.jsonl", "w") as lang_output_file:
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

        # lang_data = data["output"]["linguist"][0]
        # if lang_data is not None:
        #     lang_output_file.write(
        #         json.dumps({"commit_id": data["id"], "file_langs": lang_data}) + "\n"
        #     )
