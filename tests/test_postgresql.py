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

_schema = s.dict(
    {
        "id": s.uuid(),
        "str": s.str(),
        "dict": s.dict({"a": s.int()}),
        "list": s.list(s.int()),
        "set": s.set(s.str()),
        "int": s.int(),
        "float": s.float(),
        "bool": s.bool(),
        "bytes": s.bytes(format="binary"),
        "date": s.date(),
        "datetime": s.datetime(),
    }
)


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
    return postgresql.Table("foo", _schema, "id")


@pytest.fixture()
def resource(database, table):
    return db.TableResource(database, table)


import datetime


def test_crud(resource):
    body = {
        "id": uuid4(),
        "str": "string",
        "dict": {"a": 1},
        "list": [1, 2, 3],
        "set": {"foo", "bar"},
        "int": 1,
        "float": 2.3,
        "bool": True,
        "bytes": b"12345",
        "date": s.date().str_decode("2019-01-01"),
        "datetime": s.datetime().str_decode("2019-01-01T01:01:01Z"),
    }
    resource.create(body["id"], body)
    assert resource.read(body["id"]) == body
    body["dict"] = {"a": 2}
    body["list"] = [2, 3, 4]
    del body["set"]
    body["int"] = 2
    body["float"] = 1.0
    body["bool"] = False
    del body["bytes"]
    del body["date"]
    #   del body["datetime"]
    resource.update(body["id"], body)
    assert resource.read(body["id"]) == body
    resource.delete(body["id"])
    with pytest.raises(NotFound):
        resource.read(body["id"])


def test_list(resource):
    count = 10
    for n in range(0, count):
        id = uuid4()
        assert resource.create(id, {"id": id}) == {"id": id}
    ids = resource.list()
    assert len(ids) == count
    for id in ids:
        resource.delete(id)
    assert len(resource.list()) == 0


def test_list_where(resource):
    for n in range(0, 20):
        id = uuid4()
        assert resource.create(id, {"id": id, "int": n}) == {"id": id}
    where = resource.query()
    where.text("int < ")
    where.param(resource.table.encode("int", 10))
    ids = resource.list(where=where)
    assert len(ids) == 10
    for id in resource.list():
        resource.delete(id)
    assert len(resource.list()) == 0


def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        resource.delete(uuid4())


def test_rollback(resource):
    assert len(resource.list()) == 0
    try:
        with resource.connect():  # transaction demarcation
            id = uuid4()
            resource.create(id, {"id": id})
            assert len(resource.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(resource.list()) == 0


def test_nested_connect(database):
    with database.connect() as c1:
        with database.connect() as c2:
            assert c1 == c2
        with database.connect() as c3:
            assert c1 == c3


def test_nested_connect_rollback(resource):
    assert len(resource.list()) == 0
    id = uuid4()
    try:
        with resource.database.connect() as c1:  # transaction demarcation
            with resource.database.connect() as c2:
                resource.create(id, {"id": id})
                assert len(resource.list()) == 1
                raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(resource.list()) == 0
