"""Module to manage resource items in a PostgreSQL database."""

import contextlib
import json
import logging
import roax.schema as s
import roax.db as db
import psycopg2
import psycopg2.pool
import threading


_logger = logging.getLogger(__name__)


class _JSONAdapter:
    def encode(self, schema, value):
        return json.dumps(schema.json_encode(value))

    def decode(self, schema, value):
        return schema.json_decode(value)


class _PassAdapter:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return value


class _BytesAdapter:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return bytes(value)


_pass = _PassAdapter()
_json = _JSONAdapter()
_bytes = _BytesAdapter()
_text = db.Adapter()


_adapters = {
    s.dataclass: _json,
    s.dict: _json,
    s.str: _pass,
    s.list: _json,
    s.set: _json,
    s.int: _pass,
    s.float: _pass,
    s.bool: _pass,
    s.bytes: _bytes,
    s.date: _pass,
    s.datetime: _pass,
    s.uuid: _text,
}


class Database(db.Database):
    """Manages connections to a PostgreSQL database."""

    def __init__(self, minconn, maxconn, **kwargs):
        super().__init__(psycopg2)
        self.pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, **kwargs)
        self.local = threading.local()
        self.adapters = _adapters

    @contextlib.contextmanager
    def connect(self):
        """
        Return a context manager that yields a database connection with transaction demarcation.
        If more than one request for a connection is made in the same thread, the same connection
        will be returned; only the outermost yielded connection shall have transaction demarcation.
        """
        try:
            connection = self.local.connection
            self.local.count += 1
        except AttributeError:
            connection = self.pool.getconn(key=threading.get_ident())
            _logger.debug("%s", "psycopg2 connection begin")
            self.local.connection = connection
            self.local.count = 1
        try:
            yield connection
            if self.local.count == 1:
                _logger.debug("%s", "psycopg2 connection commit")
                connection.commit()
        except:
            if self.local.count == 1:
                _logger.debug("%s", "psycopg2 connection rollback")
                connection.rollback()
            raise
        finally:
            self.local.count -= 1
            if not self.local.count:
                del self.local.connection
                self.pool.putconn(conn=connection, key=threading.get_ident())
