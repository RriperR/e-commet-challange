from collections import Counter
from datetime import datetime, timezone
from typing import Final, Any

from aiohttp import ClientSession
from pydantic import BaseModel

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


class RepositoryAuthorCommitsNum(BaseModel):
    author: str
    commits_num: int


class Repository(BaseModel):
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str | None = None
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(self, access_token: str):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )

    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        async with self._session.request(method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params) as response:
            return await response.json()

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )
        return data["items"]

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        return await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": since.isoformat()},
        )

    async def get_repositories(self) -> list[Repository]:
        repos_data = await self._get_top_repositories()
        repositories: list[Repository] = []
        for idx, repo in enumerate(repos_data, start=1):
            owner_login = repo["owner"]["login"]
            name = repo["name"]
            commits = await self._get_repository_commits(owner_login, name)
            author_counter: Counter[str] = Counter()
            for commit in commits:
                author = commit.get("commit", {}).get("author", {}).get("name")
                if author:
                    author_counter[author] += 1
            authors = [
                RepositoryAuthorCommitsNum(author=a, commits_num=n)
                for a, n in author_counter.items()
            ]
            repositories.append(
                Repository(
                    name=name,
                    owner=owner_login,
                    position=idx,
                    stars=repo["stargazers_count"],
                    watchers=repo["watchers_count"],
                    forks=repo["forks_count"],
                    language=repo["language"],
                    authors_commits_num_today=authors,
                )
            )
        return repositories

    async def close(self):
        await self._session.close()
