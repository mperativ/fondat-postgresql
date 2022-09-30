"""Module to manage resource items in a PostgreSQL database."""

import asyncio
import asyncpg
import contextvars
import dataclasses
import fondat.codec
import fondat.error
import fondat.sql
import json
import logging
import types
import typing
import uuid

from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from fondat.codec import Codec, DecodeError, JSONCodec
from fondat.data import datacls
from fondat.sql import Expression
from fondat.types import is_optional, is_subclass, literal_values, strip_annotations
from fondat.validation import validate_arguments
from types import NoneType
from typing import Annotated, Any, Literal, TypeVar, get_args, get_origin
from uuid import UUID


_logger = logging.getLogger(__name__)


PT = TypeVar("PT")
ST = TypeVar("ST")


class PostgreSQLCodec(Codec[PT, Any]):
    """Base class for PostgreSQL codecs."""

    _cache = {}


class PassthroughCodec(PostgreSQLCodec[PT]):
    """..."""

    sql_types = {
        str: "TEXT",
        bool: "BOOLEAN",
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        bytes: "BYTEA",
        bytearray: "BYTEA",
        UUID: "UUID",
        Decimal: "NUMERIC",
        datetime: "TIMESTAMP WITH TIME ZONE",
        date: "DATE",
    }

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type in cls.sql_types.keys()

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.sql_type = self.sql_types[strip_annotations(python_type)]

    @validate_arguments
    def encode(self, value: PT) -> PT:
        return value

    @validate_arguments
    def decode(self, value: PT) -> PT:
        return value


class ArrayCodec(PostgreSQLCodec[PT]):
    """..."""

    _AVOID = str | bytes | bytearray | Mapping | tuple

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        origin = get_origin(python_type) or python_type
        args = get_args(python_type)
        return (
            is_subclass(origin, Iterable) and is_subclass(origin, cls._AVOID) and len(args) == 1
        )

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        python_type = strip_annotations(python_type)
        self.codec = PostgreSQLCodec.get(get_args(python_type)[0])
        self.sql_type = f"{self.codec.sql_type}[]"

    def encode(self, value: PT) -> Any:
        return [self.codec.encode(v) for v in value]

    def decode(self, value: Any) -> PT:
        return self.python_type(self.codec.decode(v) for v in value)


class UnionCodec(PostgreSQLCodec[PT]):
    """
    Codec that encodes/decodes a UnionType, Union or optional value to/from a compatible SQL
    value. For an optional type, it will use the codec for its type, otherwise it will
    encode/decode as JSONB.
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return typing.get_origin(python_type) in {typing.Union, types.UnionType}

    def __init__(self, python_type: type[PT]):
        super().__init__(python_type)
        raw_type = strip_annotations(python_type)
        args = typing.get_args(raw_type)
        self.is_nullable = is_optional(raw_type)
        args = [a for a in args if a is not NoneType]
        self.codec = PostgreSQLCodec.get(args[0]) if len(args) == 1 else JSONBCodec(python_type)
        self.sql_type = self.codec.sql_type

    def encode(self, value: PT) -> Any:
        if value is None:
            return None
        return self.codec.encode(value)

    def decode(self, value: Any) -> PT:
        if value is None and self.is_nullable:
            return None
        return self.codec.decode(value)


class LiteralCodec(PostgreSQLCodec[PT]):
    """
    Codec that encodes/decodes a Literal value to/from a compatible SQL value. If all literal
    values share the same type, then it will use a codec for that type, otherwise it will
    encode/decode as JSONB.
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return typing.get_origin(python_type) is Literal

    def __init__(self, python_type: type[PT]):
        super().__init__(python_type)
        self.literals = literal_values(python_type)
        types = list({type(literal) for literal in self.literals})
        self.codec = (
            PostgreSQLCodec.get(types[0]) if len(types) == 1 else JSONBCodec(python_type)
        )
        self.is_nullable = is_optional(python_type) or None in self.literals
        self.sql_type = self.codec.sql_type

    def encode(self, value: PT) -> Any:
        if value is None:
            return None
        return self.codec.encode(value)

    def decode(self, value: Any) -> PT:
        if value is None and self.is_nullable:
            return None
        result = self.codec.decode(value)
        if result not in self.literals:
            raise DecodeError
        return result


class JSONBCodec(PostgreSQLCodec[PT]):
    """
    Codec that encodes/decodes a value to/from a SQL JSONB value. This is the "fallback" codec,
    which handles any type not handled by any other codec.
    """

    sql_type = "JSONB"

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        for other in (c for c in PostgreSQLCodec.__subclasses__() if c is not JSONBCodec):
            if other.handles(python_type):
                return False
        return True

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = JSONCodec.get(python_type)

    def encode(self, value: PT) -> Any:
        return json.dumps(self.codec.encode(value))

    def decode(self, value: Any) -> PT:
        return self.codec.decode(json.loads(value))


class _Results(AsyncIterator[Any]):

    __slots__ = {"statement", "result", "rows", "codecs"}

    def __init__(self, statement, result, rows):
        self.statement = statement
        self.result = result
        self.rows = rows
        self.codecs = {
            k: PostgreSQLCodec.get(t)
            for k, t in typing.get_type_hints(result, include_extras=True).items()
        }

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.rows.__anext__()
        result = {}
        for key in self.codecs:
            with DecodeError.path_on_error(key):
                result[key] = self.codecs[key].decode(row[key])
        return self.result(**result)


# fmt: off
@datacls
class Config:
    dsn: Annotated[str | None, "connection arguments in libpg connection URI format"]
    min_size: Annotated[int | None, "number of connections to initialize pool with"]
    max_size: Annotated[int | None, "maximum number of connections in the pool"]
    max_queries: Annotated[int | None, "number of queries before connection is replaced"]
    max_inactive_connection_lifetime: Annotated[float | None, "seconds after inactive connection closed"]
    host: Annotated[str | None, "database host address"]
    port: Annotated[int | None, "port number to connect to"]
    user: Annotated[str | None, "the name of the database role used for authentication"]
    password: Annotated[str | None, "password to be used for authentication"]
    passfile: Annotated[str | None, "the name of the file used to store passwords"]
    database: Annotated[str | None, "the name of the database to connect to"]
    timeout: Annotated[float | None, "connection timeout in seconds"]
    ssl: Literal["disable", "prefer", "require", "verify-ca", "verify-full"] | None
# fmt: on


@asynccontextmanager
async def _async_null_context():
    yield


class Database(fondat.sql.Database):
    """
    Manages access to a PostgreSQL database.

    Supplied configuration can be a Config dataclass instance, or a function or coroutine
    function that returns a Config dataclass instance.
    """

    @classmethod
    async def create(cls, config: Config):
        self = cls()
        kwargs = {k: v for k, v in dataclasses.asdict(config).items() if v is not None}
        self._config = config
        self._pool = await asyncpg.create_pool(**kwargs)
        self._conn = contextvars.ContextVar("fondat_postgresql_conn", default=None)
        self._txn = contextvars.ContextVar("fondat_postgresql_txn", default=None)
        self._task = contextvars.ContextVar("fondat_postgresql_task", default=None)
        return self

    async def close(self):
        """Close all database connections."""
        if self._pool:
            await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def connection(self) -> None:
        task = asyncio.current_task()
        if self._conn.get() and self._task.get() is task:
            yield  # connection already established
            return
        _logger.debug("open connection")
        self._task.set(task)
        async with self._pool.acquire(timeout=self._config.timeout) as connection:
            self._conn.set(connection)
            try:
                yield
            finally:
                _logger.debug("close connection")
                self._conn.set(None)

    @asynccontextmanager
    async def transaction(self) -> None:
        txid = uuid.uuid4().hex
        _logger.debug("transaction begin %s", txid)
        token = self._txn.set(txid)
        async with self.connection():
            connection = self._conn.get()
            transaction = connection.transaction()
            await transaction.start()

            async def commit():
                _logger.debug("transaction commit %s", txid)
                await transaction.commit()

            async def rollback():
                _logger.debug("transaction rollback %s", txid)
                await transaction.rollback()

            try:
                yield
            except GeneratorExit:  # explicit cleanup of asynchronous generator
                await commit()
            except Exception:
                await rollback()
                raise
            else:
                await commit()
            finally:
                self._txn.reset(token)

    async def execute(
        self,
        statement: Expression,
        result: type = None,
    ) -> AsyncIterator[Any] | None:
        if not self._txn.get():
            raise RuntimeError("transaction context required to execute statement")
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(str(statement))
        text = []
        args = []
        for fragment in statement:
            if isinstance(fragment, str):
                text.append(fragment)
            else:
                args.append(PostgreSQLCodec.get(fragment.type).encode(fragment.value))
                text.append(f"${len(args)}")
        text = "".join(text)
        conn = self._conn.get()
        if result is None:
            await conn.execute(text, *args)
        else:  # expecting a result
            return _Results(statement, result, conn.cursor(text, *args).__aiter__())

    def sql_type(self, type: Any) -> str:
        return PostgreSQLCodec.get(type).sql_type


class Index(fondat.sql.Index):
    """
    Represents an index on a table in a PostgreSQL database.

    Parameters:
    • name: name of index
    • table: table that the index defined for
    • keys: index keys (typically column names with optional order)
    • unique: is index unique
    • method: indexing method
    """

    __slots__ = ("method",)

    def __init__(
        self,
        name: str,
        table: fondat.sql.Table,
        keys: Sequence[str],
        unique: bool = False,
        method: str | None = None,
    ):
        super().__init__(name, table, keys, unique)
        self.method = method

    def __repr__(self):
        result = f"Index(name={self.name}, table={self.table}, keys={self.keys}, unique={self.unique} method={self.method})"

    async def create(self):
        """Create index in database."""
        stmt = Expression()
        stmt += "CREATE "
        if self.unique:
            stmt += "UNIQUE "
        stmt += f"INDEX {self.name} ON {self.table.name} "
        if self.method:
            stmt += f"USING {self.method} "
        stmt += "("
        stmt += ", ".join(self.keys)
        stmt += ");"
        await self.table.database.execute(stmt)
