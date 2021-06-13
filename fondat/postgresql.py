"""Module to manage resource items in a PostgreSQL database."""

import asyncpg
import contextlib
import contextvars
import dataclasses
import fondat.codec
import fondat.sql
import functools
import json
import logging
import typing

from asyncio.exceptions import CancelledError
from collections.abc import AsyncIterator, Iterable
from datetime import date, datetime
from decimal import Decimal
from fondat.data import datacls
from fondat.sql import Statement
from fondat.validation import validate_arguments
from typing import Annotated as A, Any, Literal, Optional, Union
from uuid import UUID


_logger = logging.getLogger(__name__)

NoneType = type(None)


class PostgreSQLCodec(fondat.codec.Codec[fondat.codec.F, Any]):
    """Base class for PostgreSQL codecs."""


codec_providers = []


@functools.cache
def get_codec(python_type) -> PostgreSQLCodec:
    """Return a codec compatible with the specified Python type."""

    if typing.get_origin(python_type) is typing.Annotated:
        python_type = typing.get_args(python_type)[0]  # strip annotation

    for provider in codec_providers:
        if (codec := provider(python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide PostgreSQL codec for {python_type}")


def _codec_provider(wrapped=None):
    if wrapped is None:
        return functools.partial(_codec_provider)
    codec_providers.append(wrapped)
    return wrapped


def _pass_codec(python_type, sql_type):
    class PassCodec(PostgreSQLCodec[python_type]):
        @validate_arguments
        def encode(self, value: python_type) -> python_type:
            return value

        @validate_arguments
        def decode(self, value: python_type) -> python_type:
            return value

    PassCodec.sql_type = sql_type
    return PassCodec()


def _pass_codec_provider(sql_type, python_types):
    if not isinstance(python_types, Iterable):
        python_types = (python_types,)

    @_codec_provider
    def provider(python_type):
        if python_type in python_types:
            return _pass_codec(python_type, sql_type)


_pass_codec_provider("bigint", int)
_pass_codec_provider("boolean", bool)
_pass_codec_provider("bytea", (bytes, bytearray))
_pass_codec_provider("date", date)
_pass_codec_provider("double precision", float)
_pass_codec_provider("numeric", Decimal)
_pass_codec_provider("text", str)
_pass_codec_provider("timestamp with time zone", datetime)
_pass_codec_provider("uuid", UUID)


def _issubclass(cls, cls_or_tuple):
    try:
        return issubclass(cls, cls_or_tuple)
    except:
        return False


@_codec_provider
def _iterable_codec_provider(python_type):

    origin = typing.get_origin(python_type)
    if not origin or not _issubclass(origin, Iterable):
        return

    args = typing.get_args(python_type)
    if not args or len(args) > 1:
        return

    codec = get_codec(args[0])

    class IterableCodec(PostgreSQLCodec[python_type]):

        sql_type = f"{codec.sql_type}[]"

        @validate_arguments
        def encode(self, value: python_type) -> Any:
            return [codec.encode(v) for v in value]

        @validate_arguments
        def decode(self, value: Any) -> python_type:
            return python_type(codec.decode(v) for v in value)

    return IterableCodec()


@_codec_provider
def _union_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a Union or Optional value to/from a
    compatible PostgreSQL value. For Optional value, will use codec for its
    type, otherwise it encodes/decodes as jsonb.
    """

    origin = typing.get_origin(python_type)
    if origin is not Union:
        return

    args = typing.get_args(python_type)
    is_nullable = NoneType in args
    args = [a for a in args if a is not NoneType]
    codec = (
        get_codec(args[0]) if len(args) == 1 else _jsonb_codec_provider(python_type)
    )  # Optional[T]

    class UnionCodec(PostgreSQLCodec[python_type]):

        sql_type = codec.sql_type

        @validate_arguments
        def encode(self, value: python_type) -> Any:
            if value is None:
                return None
            return codec.encode(value)

        @validate_arguments
        def decode(self, value: Any) -> python_type:
            if value is None and is_nullable:
                return None
            return codec.decode(value)

    return UnionCodec()


@_codec_provider
def _literal_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a Literal value to/from a compatible
    PostgreSQL value. If all literal values share the same type, then a codec
    for that type will be used, otherwise it encodes/decodes as jsonb.
    """

    origin = typing.get_origin(python_type)
    if origin is not Literal:
        return

    return get_codec(Union[tuple(type(arg) for arg in typing.get_args(python_type))])


@_codec_provider
def _jsonb_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a PostgreSQL jsonb
    value. It unconditionally returns the codec, regardless of Python type.
    It must be the last provider in the list to serve as a catch-all.
    """

    json_codec = fondat.codec.get_codec(fondat.codec.JSON, python_type)

    class JSONBCodec(PostgreSQLCodec[python_type]):

        sql_type = "jsonb"

        @validate_arguments
        def encode(self, value: python_type) -> str:
            return json.dumps(json_codec.encode(value))

        @validate_arguments
        def decode(self, value: str) -> python_type:
            return json_codec.decode(json.loads(value))

    return JSONBCodec()


class _Results(AsyncIterator[Any]):
    def __init__(self, statement, results):
        self.statement = statement
        self.results = results
        self.codecs = {
            k: get_codec(t)
            for k, t in typing.get_type_hints(statement.result, include_extras=True).items()
        }

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.results.__anext__()
        return self.statement.result(**{k: self.codecs[k].decode(row[k]) for k in self.codecs})


@datacls
class Config:
    host: A[Optional[str], "database host address"]
    port: A[Optional[int], "port number to connect to"]
    user: A[Optional[str], "the name of the database role used for authentication"]
    password: A[Optional[str], "password to be used for authentication"]
    passfile: A[Optional[str], "the name of the file used to store passwords"]
    database: A[Optional[str], "the name of the database to connect to"]
    timeout: A[Optional[float], "connection timeout in seconds"]
    ssl: Optional[Literal["disable", "prefer", "require", "verify-ca", "verify-full"]]


def _asdict(config):
    return (
        {k: v for k, v in dataclasses.asdict(config).items() if v is not None}
        if config is not None
        else {}
    )


class Database(fondat.sql.Database):
    """
    Manages access to a PostgreSQL database.

    Parameters:
    • host: Database host address.
    • port: Port number to connect to at the server host.
    • user: The name of the database role used for authentication.
    • database: The name of the database to connect to.
    • password: Password to be used for authentication.
    • timeout: Connection timeout in seconds.
    """

    def __init__(self, config=None, **kwargs):
        super().__init__()
        self._kwargs = _asdict(config) | kwargs
        self._connection = contextvars.ContextVar("fondat_postgresql_connection")

    @contextlib.asynccontextmanager
    async def transaction(self):
        connection = self._connection.get(None)
        token = None
        if not connection:
            connection = await asyncpg.connect(**self._kwargs)
            transaction = connection.transaction()
            _logger.debug("%s", "transaction begin")
            await transaction.start()
            token = self._connection.set(connection)
        try:
            yield
        except Exception as e:

            # There is an issue in Python when a context manager is created
            # within a generator: if the generator is not iterated fully, the
            # context manager will not exit until the event loop cancels the
            # task by raising a CancelledError, long after the context is
            # assumed to be out of scope. Until there is some kind of fix,
            # this warning is an attempt to surface the problem.
            if type(e) is CancelledError:
                _logger.warning(
                    "%s",
                    "transaction failed due to CancelledError; "
                    "possible transaction context in aborted generator?",
                )

            # A GeneratorExit exception is raised when an explicit attempt
            # is made to cleanup an asynchronus generator via the aclose
            # coroutine method. Therefore, such an exception is not cause to
            # rollback the transaction.
            if token and not type(e) is GeneratorExit:
                _logger.debug("%s", "transaction rollback")
                await transaction.rollback()
                raise
        else:
            if token:
                _logger.debug("%s", "transaction commit")
                await transaction.commit()
        finally:
            if token:
                self._connection.reset(token)
                await connection.close()

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[Any]]:
        if not (connection := self._connection.get(None)):
            raise RuntimeError("transaction context required to execute statement")
        text = []
        args = []
        for fragment in statement:
            if isinstance(fragment, str):
                text.append(fragment)
            else:
                args.append(get_codec(fragment.python_type).encode(fragment.value))
                text.append(f"${len(args)}")
        text = "".join(text)
        _logger.debug("%s args=%s", text, args)
        if statement.result is None:
            await connection.execute(text, *args)
        else:
            return _Results(statement, connection.cursor(text, *args).__aiter__())

    def get_codec(self, python_type: Any) -> PostgreSQLCodec:
        return get_codec(python_type)
