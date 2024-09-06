from typing import Iterable
from github import Github, Repository, PaginatedList, Auth
from github.Repository import Repository
from github.PaginatedList import PaginatedList


class GithubDataCollector:
    def __init__(self, token: str) -> None:
        self.g = Github(auth=Auth.Token(token), per_page=100)

    def query_repositories(
        self, language: str, no_repos: int, search_filters: dict[str, str]
    ) -> Iterable[Repository]:
        knownRepoIds = []
        noDuplicates = 0
        lastCurrentEndStars = None
        currentEndStars = None
        current_search_filters = search_filters.copy()

        # GitHub API only allows 1000 results per search
        # So, we need to search multiple times
        # However, this could result in duplicates, so we need to keep track of the repo IDs
        while len(knownRepoIds) < no_repos:
            lastCurrentEndStars = currentEndStars

            # Update the search filters to only search for repos with less stars than the current end stars
            if len(knownRepoIds) > 0:
                current_search_filters["stars"] = (
                    f"{0 if currentEndStars <= 200 else 100}..{currentEndStars}"
                )
            else:
                current_search_filters["stars"] = ">100"

            items_still_needed = no_repos - len(knownRepoIds)

            print(
                f"Searching for {language} (still need {items_still_needed} repos): {current_search_filters['stars']}"
            )

            repositories: Iterable[Repository] = self.search_repositories(
                language, current_search_filters
            )
            for repo in repositories:
                if repo.id in knownRepoIds:
                    noDuplicates += 1
                    continue
                knownRepoIds.append(repo.id)
                currentEndStars = repo.stargazers_count
                yield repo

                # End the search if we have enough repos
                if len(knownRepoIds) >= no_repos:
                    break

            # Prevent a loop of only retrieving the same repos
            if lastCurrentEndStars == currentEndStars:
                print("Break due to same end stars")
                break

        print(f"Found {noDuplicates} duplicates during search!")

    def search_repositories(
        self,
        language: str,
        search_filters: dict[str, str],
    ) -> PaginatedList[Repository]:
        langQuery = (
            " ".join([f"language:{lang}" for lang in language])
            if isinstance(language, list)
            else f"language:{language}"
        )
        additional_query = " ".join(
            map(lambda item: f"{item[0]}:{item[1]}", search_filters.items())
        )
        query = f"{langQuery} {additional_query}"
        # print(f"Searched for {query}")
        return self.g.search_repositories(query=query, sort="stars", order="desc")
