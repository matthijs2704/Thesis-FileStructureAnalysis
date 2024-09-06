from data_analysis.preprocess import preprocess
from pathlib import Path
import pandas as pd
import json
import multiprocessing as mp


def update_project(project_path: Path):
    print(f"Updating {project_path.stem}")
    project_data_file = project_path / "data.jsonl"
    project_commits_file = project_path / "commits.jsonl"
    project_repo_info_file = project_path / "repo_info.json"

    df_mutations = pd.read_json(project_data_file, lines=True)
    df_commits = pd.read_json(project_commits_file, lines=True).set_index("commit_id")

    # Preprocess data if necessary
    # This will update the input files
    df_commits, df_mutations, max_file_depth = preprocess(df_commits, df_mutations)
    df_commits.reset_index().to_json(project_commits_file, orient="records", lines=True)
    df_mutations.to_json(project_data_file, orient="records", lines=True)

    # Update repo info
    with project_repo_info_file.open("r+") as f:
        repo_info = json.load(f)
        repo_info["max_file_depth"] = int(max_file_depth)
        f.seek(0)
        json.dump(repo_info, f)


if __name__ == "__main__":
    with mp.Pool(8) as pool:
        pool.map(
            update_project,
            [
                project_path
                for project_path in Path("output").iterdir()
                if project_path.is_dir() and (project_path / "data.jsonl").exists()
            ],
        )
