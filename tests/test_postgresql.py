import pytest

import asyncio
import fondat.postgresql
import fondat.sql as sql
import logging

from datetime import date, datetime
from fondat.data import datacls, make_datacls
from typing import Optional, TypedDict
from uuid import UUID, uuid4


_logger = logging.getLogger(__name__)

pytestmark = pytest.mark.asyncio


@datacls
class DC:
    key: UUID
    str_: Optional[str]
    dict_: Optional[TypedDict("TD", {"a": int})]
    list_: Optional[list[int]]
    set_: Optional[set[str]]
    int_: Optional[int]
    float_: Optional[float]
    bool_: Optional[bool]
    bytes_: Optional[bytes]
    date_: Optional[date]
    datetime_: Optional[datetime]


config = fondat.postgresql.Config(
    database="fondat",
    user="fondat",
    password="fondat",
)


@pytest.fixture(scope="function")
def database():
    yield fondat.postgresql.Database(config)


@pytest.fixture(scope="function")
async def table(database):
    foo = sql.Table("foo", database, DC, "key")
    async with database.transaction():
        await foo.create()
    yield foo
    async with database.transaction():
        await foo.drop()


async def test_database_config_dataclass():
    database = fondat.postgresql.Database(config=config)
    async with database.transaction():
        stmt = sql.Statement()
        stmt.text(f"SELECT 1;")
        await database.execute(stmt)


async def test_database_config_function():
    def config_fn():
        return config
    database = fondat.postgresql.Database(config=config_fn)
    async with database.transaction():
        stmt = sql.Statement()
        stmt.text(f"SELECT 1;")
        await database.execute(stmt)


async def test_database_config_coroutine_function():
    async def config_corofn():
        return config
    database = fondat.postgresql.Database(config=config_corofn)
    async with database.transaction():
        stmt = sql.Statement()
        stmt.text(f"SELECT 1;")
        await database.execute(stmt)


async def test_crud(table):
    async with table.database.transaction():
        body = DC(
            key=UUID("5ea2c35d-7f88-4fca-b76f-e4482f0b28a4"),
            str_="string",
            dict_={"a": 1},
            list_=[1, 2, 3],
            set_={"foo", "bar"},
            int_=1,
            float_=2.3,
            bool_=True,
            bytes_=b"12345",
            date_=date.fromisoformat("2019-01-01"),
            datetime_=datetime.fromisoformat("2019-01-01T01:01:01+00:00"),
        )
        await table.insert(body)
        assert await table.read(body.key) == body
        body.dict_ = {"a": 2}
        body.list_ = [2, 3, 4]
        body.set_ = None
        body.int_ = 2
        body.float_ = 1.0
        body.bool_ = False
        body.bytes_ = None
        body.date_ = None
        await table.update(body)
        assert await table.read(body.key) == body
        await table.delete(body.key)
        assert await table.read(body.key) is None


async def test_list(table):
    async with table.database.transaction():
        count = 10
        for _ in range(0, count):
            body = DC(key=uuid4())
            await table.insert(body)
        results = await table.select(columns="key")
        keys = [result["key"] async for result in results]
        assert len(keys) == count
        for key in keys:
            await table.delete(key)
        assert await table.count() == 0


async def test_list_where(table):
    async with table.database.transaction():
        for n in range(0, 20):
            body = DC(key=uuid4(), int_=n)
            await table.insert(body)
        where = sql.Statement()
        where.text("int_ < ")
        where.param(10)
        results = await table.select(columns="key", where=where)
        keys = [result["key"] async for result in results]
        assert len(keys) == 10
        for key in keys:
            await table.delete(key)
        assert await table.count() == 10


async def test_rollback(table):
    async with table.database.transaction():
        assert await table.count() == 0
    try:
        async with table.database.transaction():
            body = DC(key=uuid4())
            await table.insert(body)
            assert await table.count() == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    async with table.database.transaction():
        assert await table.count() == 0


def test_consecutive_loop(database):
    @sql.transaction(database=database)
    async def select():
        stmt = sql.Statement()
        stmt.text("SELECT 1 AS foo;")
        stmt.result = make_datacls("DC", (("foo", int),))
        result = await (await database.execute(stmt)).__anext__()
        assert result.foo == 1

    asyncio.run(select())
    asyncio.run(select())


async def test_gather(database):
    async def select(n: int):
        async with database.transaction():
            stmt = sql.Statement()
            stmt.text(f"SELECT {n} AS foo;")
            stmt.result = make_datacls("DC", (("foo", int),))
            async with database.transaction() as transaction:
                result = await (await database.execute(stmt)).__anext__()
                assert result.foo == n

    await asyncio.gather(*[select(n) for n in range(0, 50)])


async def test_nested_transaction(table):
    async with table.database.transaction():
        assert await table.count() == 0
        await table.insert(DC(key=uuid4()))
        assert await table.count() == 1
        try:
            async with table.database.transaction():
                await table.insert(DC(key=uuid4()))
                assert await table.count() == 2
                raise RuntimeError
        except RuntimeError:
            pass
        assert await table.count() == 1


async def test_no_connection(database):
    stmt = sql.Statement()
    stmt.text(f"SELECT 1;")
    with pytest.raises(RuntimeError):
        await database.execute(stmt)


async def test_no_transaction(database):
    async with database.connection():
        stmt = sql.Statement()
        stmt.text(f"SELECT 1;")
        with pytest.raises(RuntimeError):
            await database.execute(stmt)


async def connection_decorator(table):
    @sql.connection
    async def foo():
        key = uuid4()
        await table.insert(DC(key=key))
        assert await table.read(key)

    await foo()


async def transaction_decorator(table):
    key = uuid4()

    @sql.transaction
    async def foo():
        await table.insert(DC(key=key))
        assert await table.read(key) is not None
        raise RuntimeError  # rollback

    try:
        await foo()
    except RuntimeError:
        pass
    async with table.database.transaction():
        assert table.read(key) is None
