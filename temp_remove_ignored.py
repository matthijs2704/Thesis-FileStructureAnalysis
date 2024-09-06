import json
import os

with open("labs/ignored_repo_ids.json", "r") as f:
    ignored_repo_ids = json.load(f)

# Remove ignored projects
for repo_id in ignored_repo_ids:
    repo_path = f"/home/mcs001/20172091/thesis/raw-output/{repo_id}"
    if os.path.exists(repo_path):
        os.system(f"rm -rf {repo_path}")
        print(f"Removed {repo_id}")
