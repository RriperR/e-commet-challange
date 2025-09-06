import asyncio
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Any

from aiohttp import ClientSession
from aiochclient import ChClient

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / '2'))
from main import GithubReposScrapper  # type: ignore

BATCH_SIZE = 100


async def _insert_batch(
    client: ChClient, query: str, rows: Iterable[tuple[Any, ...]], batch_size: int = BATCH_SIZE
) -> None:
    """Insert rows into ClickHouse in batches to limit memory usage."""
    batch: list[tuple[Any, ...]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            await client.execute(query, batch)
            batch.clear()
    if batch:
        await client.execute(query, batch)


async def save_repositories(clickhouse_url: str, github_token: str) -> None:
    scrapper = GithubReposScrapper(github_token)
    repositories = await scrapper.get_repositories()
    await scrapper.close()

    now = datetime.utcnow()
    today = date.today()

    async with ClientSession() as http_session:
        ch = ChClient(http_session, url=clickhouse_url)

        repo_rows = [
            (r.name, r.owner, r.stars, r.watchers, r.forks, r.language or '', now)
            for r in repositories
        ]
        await _insert_batch(ch, "INSERT INTO test.repositories VALUES", repo_rows)

        position_rows = [
            (today, f"{r.owner}/{r.name}", r.position)
            for r in repositories
        ]
        await _insert_batch(ch, "INSERT INTO test.repositories_positions VALUES", position_rows)

        author_rows: list[tuple[Any, ...]] = []
        for repo in repositories:
            repo_name = f"{repo.owner}/{repo.name}"
            for author in repo.authors_commits_num_today:
                author_rows.append((today, repo_name, author.author, author.commits_num))
        await _insert_batch(ch, "INSERT INTO test.repositories_authors_commits VALUES", author_rows)


if __name__ == "__main__":
    ch_url = "http://localhost:8123"
    gh_token = "YOUR_GITHUB_TOKEN"
    asyncio.run(save_repositories(ch_url, gh_token))