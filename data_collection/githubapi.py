import requests
import csv
from typing import Iterable, List, Generic

from github import Github, Repository, PaginatedList

top_languages = [
    "Python",
    "Javascript",
    "Java",
    "C#",
    ["C", "C++"],
    "Typescript",
    "PHP",
    "Rust",
    "Swift",
    "Go",
]

search_filters = {"pushed": ">2020-01-01", "stars": ">100"}


class GithubDataCollector:
    def __init__(self) -> None:
        self.g = Github(
            login_or_token="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", per_page=1000
        )

    def search_repositories(
        self, language: str, search_filters: dict[str, str]
    ) -> PaginatedList[Repository.Repository]:
        langQuery = (
            f"language:{language}"
            if language != ["C", "C++"]
            else "language:C language:C++"
        )
        additional_query = " ".join(
            map(lambda k, v: f"{k}:{v}", search_filters.items())
        )
        query = f"{langQuery} {additional_query}"
        return self.g.search_repositories(query=query, sort="stars", order="desc")

    def retieve_dataset(
        self, languages: Iterable[str], no_repos: int, search_filters: dict[str, str]
    ):
        for language in languages:
            repos: PaginatedList[Repository.Repository]
            repos = self.search_repositories(language, search_filters)
            print(f"Total number of {language} repositories: {repos.totalCount}")

            with open(
                f"../output/data_top_{no_repos}/top_repos_{language}.csv",
                mode="w",
                newline="",
            ) as csv_file:
                fieldnames = [
                    "name",
                    "url",
                    "stargazers_count",
                    "owner.name",
                    "owner.type",
                    "description",
                    "language",
                    "size",
                    "archived",
                    "created_at",
                    "pushed_at",
                ]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                writer.writeheader()

                repo: Repository
                for repo in repos[:no_repos]:
                    writer.writerow(
                        {
                            "name": repo.name,
                            "url": repo.html_url,
                            "stargazers_count": repo.stargazers_count,
                            "owner.name": repo.owner.login,
                            "owner.type": repo.owner.type,
                            "description": repo.description,
                            "language": repo.language,
                            "size": repo.size,
                            "archived": repo.archived,
                            "created_at": repo.created_at,
                            "pushed_at": repo.pushed_at,
                        }
                    )
