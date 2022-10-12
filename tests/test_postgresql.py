import asyncio
import fondat.postgresql as postgresql
import fondat.sql as sql
import pytest

from copy import copy
from datetime import date, datetime
from decimal import Decimal
from fondat.data import datacls, make_datacls
from fondat.sql import Expression, Param
from typing import Literal, TypedDict
from uuid import UUID, uuid4


@datacls
class DC:
    key: UUID
    str_: str | None
    dict_: TypedDict("TD", {"a": int}) | None
    list_: list[int] | None
    set_: set[str] | None
    int_: int | None
    float_: float | None
    bool_: bool | None
    bytes_: bytes | None
    date_: date | None
    datetime_: datetime | None
    str_literal: Literal["a", "b", "c"] | None
    int_literal: Literal[1, 2, 3] | None
    mixed_literal: Literal["a", 1, True] | None


config = postgresql.Config(
    database="fondat",
    user="fondat",
    password="fondat",
)


@pytest.fixture(scope="module")
def event_loop():
    return asyncio.new_event_loop()


@pytest.fixture(scope="module")
async def database():
    db = await postgresql.Database.create(config)
    yield db
    await db.close()


@pytest.fixture(scope="function")
async def table(database):
    async with database.transaction():
        await database.execute(Expression("DROP TABLE IF EXISTS foo;"))
    foo = sql.Table("foo", database, DC, "key")
    async with database.transaction():
        await foo.create()
    yield foo
    async with database.transaction():
        await foo.drop()


async def test_pool_timeout():
    conf = copy(config)
    conf.timeout = 0.1
    conf.min_size = 1
    conf.max_size = 1
    database = await postgresql.Database.create(conf)

    async def useit():
        async with database.connection():
            await asyncio.sleep(0.2)

    with pytest.raises(asyncio.exceptions.TimeoutError):
        await asyncio.gather(*[useit() for _ in range(2)])


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
            str_literal="a",
            int_literal=2,
            mixed_literal=1,
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
        body.str_literal = None
        body.int_literal = None
        body.mixed_literal = None
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
                where=Expression("int_ < ", Param(10)),
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
        stmt = Expression(f"SELECT {n} AS foo;")
        async with database.transaction():
            result = await (
                await database.execute(stmt, make_datacls("DC", (("foo", int),)))
            ).__anext__()
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
        await database.execute(Expression(f"SELECT 1;"))


async def test_no_transaction(database):
    async with database.connection():
        stmt = sql.Expression(f"SELECT 1;")
        with pytest.raises(RuntimeError):
            await database.execute(stmt)


async def test_str_literal():
    codec = postgresql.PostgreSQLCodec.get(Literal["a", "b", "c"])
    assert codec.sql_type == "TEXT"


async def test_int_literal():
    codec = postgresql.PostgreSQLCodec.get(Literal[1, 2, 3])
    assert codec.sql_type == "BIGINT"


async def test_mixed_literal():
    codec = postgresql.PostgreSQLCodec.get(Literal["a", 1, True])
    assert codec.sql_type == "JSONB"


async def test_list():
    codec = postgresql.PostgreSQLCodec.get(list[int])
    assert codec.sql_type == "BIGINT[]"


async def test_set():
    codec = postgresql.PostgreSQLCodec.get(set[str])
    assert codec.sql_type == "TEXT[]"


async def test_passthrough_types(database):
    values = (
        True,
        b"abc",
        bytearray(b"123"),
        date.fromisoformat("2022-10-10"),
        datetime.fromisoformat("2022-10-10T10:10:10+00:00"),
        Decimal("1.23"),
        4.56,
        7,
        "text",
        UUID("24161393-4d58-4eda-bf61-ff6c20718b15"),
    )
    for value in values:
        async with database.transaction():
            stmt = Expression(
                f"SELECT ", Param(value), f"::{database.sql_type(type(value))} AS value;"
            )
            results = await database.execute(stmt, TypedDict("TD", {"value": type(value)}))
            result = await results.__anext__()
            assert issubclass(type(result["value"]), type(value))
            assert result["value"] == value


async def test_upsert(table):
    key = uuid4()
    row = DC(
        key=key,
        str_="string",
    )
    async with table.database.transaction():
        await table.upsert(row)
        read = await table.read(row.key)
        assert read.str_ == "string"
        row.str_ = "bling"
        await table.upsert(row)
        read = await table.read(row.key)
        assert read.str_ == "bling"
