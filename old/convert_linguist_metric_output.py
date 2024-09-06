import sys
import json

line = sys.stdin.readline()
data = json.loads(line)

file_types = {}

for obj in data["objects"]:
    metric = obj["metrics"][0]
    if metric["cached"]:
        continue
    # data = {"name": obj["name"], "language": metric["value"]}
    file_types[obj["name"]] = metric["value"]

print(json.dumps({"id": data["id"], "files": file_types}))
