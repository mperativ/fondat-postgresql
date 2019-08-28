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


class _JSONCodec:
    def encode(self, schema, value):
        return json.dumps(schema.json_encode(value))

    def decode(self, schema, value):
        return schema.json_decode(value)


class _TextCodec:
    def encode(self, schema, value):
        return schema.str_encode(value)

    def decode(self, schema, value):
        return schema.str_decode(value)


class _PassCodec:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return value


class _BytesCodec:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return bytes(value)


_pass = _PassCodec()
_json = _JSONCodec()
_bytes = _BytesCodec()
_text = _TextCodec()


_codecs = {
    s.dict: _json,
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
    """TODO: Description."""

    def __init__(self, minconn, maxconn, **kwargs):
        """
        :param minconn: TODO.
        :param maxconn: TODO.

        Connection keyword arguments are also supported. Some of them are:
        :param host: TODO.
        :param port: TODO.
        :param dbname: TODO.
        :param user: TODO.
        :param password: TODO.
        :param sslmode: TODO.
        :param sslrootcert: TODO.
        :param sslcert: TODO.
        :param sslkey: TODO.
        """
        super().__init__(psycopg2)
        self.pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, **kwargs)
        self.local = threading.local()

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


class Table(db.Table):
    """TODO: Description."""

    def __init__(self, name, schema, pk, codecs=None):
        """
        :param module: Module that implements the DB-API interface.
        :param name: Name of table in the SQL database.
        :param schema: Schema of table columns.
        :param primary_key: Column name of the primary key.
        :param codecs: TODO.
        """
        super().__init__(name, schema, pk, {**_codecs, **(codecs if codecs else {})})
