import dataclasses
import pytest
import roax.db as db
import roax.postgresql as postgresql
import roax.resource as r
import roax.schema as s

from datetime import date, datetime
from roax.resource import NotFound
from uuid import uuid4

import logging

_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DC:
    id: s.uuid()
    str: s.str()
    dict: s.dict({"a": s.int()})
    list: s.list(s.int())
    set: s.set(s.str())
    int: s.int()
    float: s.float()
    bool: s.bool()
    bytes: s.bytes(format="binary")
    date: s.date()
    datetime: s.datetime()


_schema = s.dataclass(DC)


@pytest.fixture(scope="module")
def database():
    db = postgresql.Database(minconn=1, maxconn=10, dbname="roax_postgresql")
    with db.cursor() as cursor:
        cursor.execute(
            """
                CREATE TABLE FOO (
                    id TEXT,
                    str TEXT,
                    dict JSONB,
                    list JSONB,
                    set JSONB,
                    int INTEGER,
                    float FLOAT,
                    bool BOOLEAN,
                    bytes BYTEA,
                    date DATE,
                    datetime TIMESTAMP WITH TIME ZONE
                );
            """
        )
    yield db
    with db.cursor() as cursor:
        cursor.execute("DROP TABLE FOO;")


@pytest.fixture()
def table(database):
    with database.cursor() as cursor:
        cursor.execute("DELETE FROM FOO;")
    return db.Table(database, "foo", _schema, "id")


@pytest.fixture()
def resource(table):
    return db.TableResource(table)


import datetime


def test_crud(resource):
    body = DC(
        id=uuid4(),
        str="string",
        dict={"a": 1},
        list=[1, 2, 3],
        set={"foo", "bar"},
        int=1,
        float=2.3,
        bool=True,
        bytes=b"12345",
        date=s.date().str_decode("2019-01-01"),
        datetime=s.datetime().str_decode("2019-01-01T01:01:01Z"),
    )
    resource.create(body.id, body)
    assert resource.read(body.id) == body
    body.dict = {"a": 2}
    body.list = [2, 3, 4]
    body.set = None
    body.int = 2
    body.float = 1.0
    body.bool = False
    body.bytes = None
    body.date = None
    resource.update(body.id, body)
    assert resource.read(body.id) == body
    resource.delete(body.id)
    with pytest.raises(NotFound):
        resource.read(body.id)


def test_list(table, resource):
    count = 10
    for n in range(0, count):
        id = uuid4()
        body = DC(
            id=id,
            str=None,
            dict=None,
            list=None,
            set=None,
            int=None,
            float=None,
            bool=None,
            bytes=None,
            date=None,
            datetime=None,
        )
        assert resource.create(id, body) == {"id": id}
    ids = table.list()
    assert len(ids) == count
    for id in ids:
        resource.delete(id)
    assert len(table.list()) == 0


def test_list_where(table, resource):
    for n in range(0, 20):
        id = uuid4()
        body = DC(
            id=id,
            str=None,
            dict=None,
            list=None,
            set=None,
            int=n,
            float=None,
            bool=None,
            bytes=None,
            date=None,
            datetime=None,
        )
        assert resource.create(id, body) == {"id": id}
    where = table.query()
    where.text("int < ")
    where.value("int", 10)
    ids = table.list(where=where)
    assert len(ids) == 10
    for id in table.list():
        resource.delete(id)
    assert len(table.list()) == 0


def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        resource.delete(uuid4())


def test_rollback(database, table, resource):
    assert len(table.list()) == 0
    try:
        with database.connect():  # transaction demarcation
            body = DC(
                id=uuid4(),
                str=None,
                dict=None,
                list=None,
                set=None,
                int=None,
                float=None,
                bool=None,
                bytes=None,
                date=None,
                datetime=None,
            )
            resource.create(body.id, body)
            assert len(table.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(table.list()) == 0


def test_nested_connect(database):
    with database.connect() as c1:
        with database.connect() as c2:
            assert c1 == c2
        with database.connect() as c3:
            assert c1 == c3


def test_nested_connect_rollback(database, table, resource):
    assert len(table.list()) == 0
    try:
        with database.connect() as c1:  # transaction demarcation
            with database.connect() as c2:
                body = DC(
                    id=uuid4(),
                    str=None,
                    dict=None,
                    list=None,
                    set=None,
                    int=None,
                    float=None,
                    bool=None,
                    bytes=None,
                    date=None,
                    datetime=None,
                )
                resource.create(body.id, body)
                assert len(table.list()) == 1
                raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(table.list()) == 0
