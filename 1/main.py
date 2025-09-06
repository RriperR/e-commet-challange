from typing import Annotated, AsyncIterator

import os
from asyncpg import connect
from asyncpg.connection import Connection
import uvicorn
from fastapi import APIRouter, FastAPI, Depends


async def get_pg_connection() -> AsyncIterator[Connection]:
    conn = await connect(
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "postgres"),
    )
    try:
        yield conn
    finally:
        await conn.close()


async def get_db_version(conn: Annotated[Connection, Depends(get_pg_connection)]):
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet")
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
