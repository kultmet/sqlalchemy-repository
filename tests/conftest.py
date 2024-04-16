import asyncio
import datetime
import json
import os
from typing import AsyncGenerator
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Connection, create_engine, delete, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeMeta, declarative_base, sessionmaker


sqlite_filename = "tests/sql_app.db"
sqlite_filename_for_settings = "settings.db"

DB_NAME = "postgres"
DB_USERNAME = "postgres"
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = "localhost"  # "postgres"
DB_PORT = 5432

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

ASYNC_DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine_test = create_engine(SQLALCHEMY_DATABASE_URL)

TEST_REDIS = os.getenv("TEST_REDIS_HOST", default="localhost")
override_redis = Redis(host=TEST_REDIS, port=6379, decode_responses=True)
keys = ["on_way_options"]

async_engine_test = create_async_engine(ASYNC_DATABASE_URL)
async_session_maker = sessionmaker(
    async_engine_test, class_=AsyncSession, expire_on_commit=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_test)


async def override_get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


def override_get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[get_async_session] = override_get_async_session


@pytest.fixture(autouse=True, scope="session")
def prepare_datebase():
    print("engine_test.url", engine_test.url)
    try:
        os.system("alembic upgrade head")
        yield
        os.system("alembic downgrade base")
    except Exception as e:
        print(e)
    finally:
        os.system("alembic downgrade base")


@pytest.fixture(autouse=True, scope="session")
def mock_redis_utils():
    from src.signals import routers

    routers.redis = override_redis
    yield


@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


client = TestClient(app)


async def ac() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="https://test") as cli:
        yield cli
