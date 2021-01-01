import dataclasses
import pytest
import fondat.postgres
import fondat.sql

from datetime import date, datetime
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


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
def database():
    yield fondat.postgres.Database(database="fondat_postgres")


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def table(database):
    foo = fondat.sql.Table("foo", database, DC, "key")
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
        results = await table.select("key")
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
        results = await table.select("key", where)
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
