from typing import List
import os
from dotenv import load_dotenv
import json
import argparse
import tempfile
from pathlib import Path
import multiprocessing as mp
from project_selection.github_data_collector import GithubDataCollector
import json
from pathlib import Path
import os
from project_selection.filter_projects import is_project_filtered
import time
import csv
import pandas as pd

args = argparse.ArgumentParser(
    description="Process language selection and output dataset."
)
args.add_argument(
    "language_set", type=Path, help="language selection, from top_10_languages.json"
)
args.add_argument("output_dataset", type=Path, help="output dataset")

input_args = args.parse_args()
language_set: Path = input_args.language_set
dataset_path: Path = input_args.output_dataset

with open(language_set, "r") as f:
    languages = json.load(f)["top_10"]
    # Split combined languages by / into a list of languages, otherwise single string
    languages = list(map(lambda x: x.split("/") if "/" in x else x, languages))

search_filters = {"pushed": ">2020-01-01"}
top_x = 10000

def query_repositories(
    token: str, search_filters: dict[str, str], output_dir: Path, n: int, language: str
) -> None:
    coll = GithubDataCollector(token)

    lang_name = "_".join(language) if isinstance(language, list) else language
    file_path = output_dir / f"repos_{lang_name.lower()}.csv"

    if os.path.exists(file_path):
        os.remove(file_path)

    with open(
        file_path,
        mode="w",
        newline="",
    ) as csv_file:
        fieldnames = [
            "name",
            "url",
            "clone_url",
            "default_branch",
            "stargazers_count",
            "owner.name",
            "owner.type",
            "description",
            "language",
            "topics",
            "size",
            "archived",
            "created_at",
            "pushed_at",
            "ignored",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()

        start_time = time.time()

        repos = coll.query_repositories(language, n, search_filters)
        no_repos = 0
        for repo in repos:
            project = {
                "name": repo.name,
                "url": repo.html_url,
                "clone_url": repo.clone_url,
                "default_branch": repo.default_branch,
                "stargazers_count": repo.stargazers_count,
                "owner.name": repo.owner.login,
                "owner.type": repo.owner.type,
                "description": repo.description,
                "language": repo.language,
                "topics": "|".join(repo.topics),
                "size": repo.size,
                "archived": repo.archived,
                "created_at": repo.created_at,
                "pushed_at": repo.pushed_at,
            }
            project["ignored"] = is_project_filtered(project)
            writer.writerow(project)
            no_repos += 1

        print(f"Finished {language} ({no_repos}) in {time.time() - start_time}s")


def combine_datasets(input_dir: Path, output_file: Path, languages: list[str]):
    all_data = []

    for lang in languages:
        language_name = "_".join(lang) if isinstance(lang, list) else lang
        # Load dataset for Python
        df = pd.read_csv(input_dir / f"repos_{language_name.lower()}.csv")
        df["lang"] = language_name
        all_data.append(df)

    combined_df = pd.concat(all_data)
    combined_df.to_csv(output_file, index=False)

def procRun(token: str, tmp_dir: str, langs: List[str]) -> None:
    for lang in langs:
        query_repositories(token, search_filters, Path(tmp_dir), top_x, lang)
        
if __name__ == "__main__":
    config = load_dotenv()
    token = os.getenv("THESIS_GITHUB_TOKEN")
    token2 = os.getenv("THESIS_GITHUB_TOKEN_2")

    expected_sec_to_completion = (
        len(languages) * 350
    )  # alternative, with MP: * 10000 / 100 / 30 * 60
    print("Estimated time to completion: " + str(expected_sec_to_completion) + "s")

    with tempfile.TemporaryDirectory() as tmp_dir:
        

        half = len(languages) // 2
        langs_1 = languages[:half]
        langs_2 = languages[half:]

        proc1 = mp.Process(target=procRun, args=(token, tmp_dir, langs_1,))
        proc2 = mp.Process(target=procRun, args=(token2, tmp_dir, langs_2,))
        proc1.start()
        proc2.start()
        proc1.join()
        proc2.join()

        # for lang in languages:
        #     query_repositories(token, search_filters, Path(tmp_dir), top_x, lang)

        combine_datasets(Path(tmp_dir), dataset_path, languages)
