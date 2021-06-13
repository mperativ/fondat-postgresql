import pytest

import asyncio
import dataclasses
import fondat.postgresql
import fondat.sql

from datetime import date, datetime
from fondat.data import make_datacls
from fondat.sql import Parameter, Statement
from typing import Optional, TypedDict
from uuid import UUID, uuid4


pytestmark = pytest.mark.asyncio


@dataclasses.dataclass
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
    foo = fondat.sql.Table("foo", database, DC, "key")
    async with database.transaction():
        try:
            await foo.drop()
        except:
            pass
    async with database.transaction():
        await foo.create()
    yield foo
    async with database.transaction():
        await foo.drop()


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
        for n in range(0, count):
            body = DC(
                key=uuid4(),
                str_=None,
                dict_=None,
                list_=None,
                set_=None,
                int_=None,
                float_=None,
                bool_=None,
                bytes_=None,
                date_=None,
                datetime_=None,
            )
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
            body = DC(
                key=uuid4(),
                str_=None,
                dict_=None,
                list_=None,
                set_=None,
                int_=n,
                float_=None,
                bool_=None,
                bytes_=None,
                date_=None,
                datetime_=None,
            )
            await table.insert(body)
        where = Statement()
        where.text("int_ < ")
        where.param(10)
        results = await table.select(columns="key", where=where)
        keys = [result["key"] async for result in results]
        assert len(keys) == 10
        for key in keys:
            await table.delete(key)
        assert await table.count() == 10


async def test_rollback(database, table):
    async with database.transaction():
        assert await table.count() == 0
    try:
        async with database.transaction():
            body = DC(
                key=uuid4(),
                str_=None,
                dict_=None,
                list_=None,
                set_=None,
                int_=None,
                float_=None,
                bool_=None,
                bytes_=None,
                date_=None,
                datetime_=None,
            )
            await table.insert(body)
            assert await table.count() == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    async with database.transaction():
        assert await table.count() == 0


def test_consecutive_loop(database):
    async def select():
        stmt = Statement()
        stmt.text("SELECT 1 AS foo;")
        stmt.result = make_datacls("DC", (("foo", int),))
        async with database.transaction() as transaction:
            result = await (await database.execute(stmt)).__anext__()
            assert result.foo == 1

    asyncio.run(select())
    asyncio.run(select())


async def test_gather(database):
    count = 50

    async def select(n: int):
        stmt = Statement()
        stmt.text(f"SELECT {n} AS foo;")
        stmt.result = make_datacls("DC", (("foo", int),))
        async with database.transaction() as transaction:
            result = await (await database.execute(stmt)).__anext__()
            assert result.foo == n

    await asyncio.gather(*[select(n) for n in range(0, count)])
