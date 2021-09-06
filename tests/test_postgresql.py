import pytest

import asyncio
import fondat.postgresql
import fondat.sql as sql
import logging

from datetime import date, datetime
from fondat.data import datacls, make_datacls
from fondat.sql import Statement, Expression, Param
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


@pytest.fixture(scope="module")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="module")
async def database():
    db = await fondat.postgresql.Database.create(config)
    yield db
    await db.close()


@pytest.fixture(scope="function")
async def table(database):
    foo = sql.Table("foo", database, DC, "key")
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
        for _ in range(0, count):
            body = DC(key=uuid4())
            await table.insert(body)
        keys = [row["key"] async for row in table.select(columns="key")]
        assert len(keys) == count
        for key in keys:
            await table.delete(key)
        assert await table.count() == 0


async def test_list_where(table):
    async with table.database.transaction():
        for n in range(0, 20):
            body = DC(key=uuid4(), int_=n)
            await table.insert(body)
        keys = [
            row["key"]
            async for row in table.select(
                columns="key",
                where=Expression(
                    "int_ < ",
                    Param(
                        10,
                    ),
                ),
            )
        ]
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


async def test_gather(database):
    async def select(n: int):
        stmt = Statement(f"SELECT {n} AS foo;", result=make_datacls("DC", (("foo", int),)))
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
    with pytest.raises(RuntimeError):
        await database.execute(Statement(f"SELECT 1;"))


async def test_no_transaction(database):
    async with database.connection():
        stmt = sql.Statement(f"SELECT 1;")
        with pytest.raises(RuntimeError):
            await database.execute(stmt)


async def test_foo(database):
    DC = make_datacls("DC", (("id", str), ("val", list[str])))
    table = sql.Table(name="fun", database=database, schema=DC, pk="id")
    index = fondat.postgresql.Index(name="fun_ix", table=table, keys=("val",), method="GIN")
    async with database.transaction():
        await table.create()
        await index.create()
        await table.drop()
