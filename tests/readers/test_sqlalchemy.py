# pylint: disable=missing-docstring

import pytest
from sqlalchemy import (
    CHAR,
    DECIMAL,
    NCHAR,
    NVARCHAR,
    TIMESTAMP,
    VARCHAR,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    Table,
    Text,
    Time,
    create_engine,
)

from recap.readers.sqlalchemy import SqlAlchemyReader
from recap.types import BoolType, BytesType, FloatType, IntType, StringType

metadata = MetaData()

test_table = Table(
    "test_table",
    metadata,
    Column("column_int", Integer),
    Column("column_bigint", BigInteger),
    Column("column_smallint", SmallInteger),
    Column("column_float", Float),
    Column("column_real", Float),
    Column("column_boolean", Boolean),
    Column("column_varchar", VARCHAR),
    Column("column_text", Text),
    Column("column_nvarchar", NVARCHAR),
    Column("column_char", CHAR),
    Column("column_nchar", NCHAR),
    Column("column_date", Date),
    Column("column_time", Time),
    Column("column_datetime", DateTime),
    Column("column_timestamp", TIMESTAMP),
    Column("column_binary", LargeBinary),
    Column("column_decimal", DECIMAL),
)


@pytest.fixture(scope="module")
def engine():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


def test_sqlalchemy_reader(engine):
    reader = SqlAlchemyReader(engine)
    struct = reader.struct("test_table")

    # Now we validate the returned StructType
    assert struct.extra_attrs["name"] == "test_table"
    assert len(struct.fields) == 17

    int_type_field = struct.fields[0]
    assert isinstance(int_type_field, IntType)
    assert int_type_field.extra_attrs["name"] == "column_int"
    assert int_type_field.bits == 32
    assert int_type_field.signed is True

    bigint_type_field = struct.fields[1]
    assert isinstance(bigint_type_field, IntType)
    assert bigint_type_field.extra_attrs["name"] == "column_bigint"
    assert bigint_type_field.bits == 64
    assert bigint_type_field.signed is True

    smallint_type_field = struct.fields[2]
    assert isinstance(smallint_type_field, IntType)
    assert smallint_type_field.extra_attrs["name"] == "column_smallint"
    assert smallint_type_field.bits == 16
    assert smallint_type_field.signed is True

    float_type_field = struct.fields[3]
    assert isinstance(float_type_field, FloatType)
    assert float_type_field.extra_attrs["name"] == "column_float"
    assert float_type_field.bits == 64

    # SQLite doesn't have a REAL type, so it's mapped to FLOAT
    float_type_field = struct.fields[4]
    assert isinstance(float_type_field, FloatType)
    assert float_type_field.extra_attrs["name"] == "column_real"
    assert float_type_field.bits == 64

    float_type_field = struct.fields[5]
    assert isinstance(float_type_field, BoolType)
    assert float_type_field.extra_attrs["name"] == "column_boolean"

    varchar_type_field = struct.fields[6]
    assert isinstance(varchar_type_field, StringType)
    assert varchar_type_field.extra_attrs["name"] == "column_varchar"
    assert varchar_type_field.bytes_ is None

    text_type_field = struct.fields[7]
    assert isinstance(text_type_field, StringType)
    assert text_type_field.extra_attrs["name"] == "column_text"
    assert text_type_field.bytes_ is None

    nvarchar_type_field = struct.fields[8]
    assert isinstance(nvarchar_type_field, StringType)
    assert nvarchar_type_field.extra_attrs["name"] == "column_nvarchar"
    assert nvarchar_type_field.bytes_ is None

    char_type_field = struct.fields[9]
    assert isinstance(char_type_field, StringType)
    assert char_type_field.extra_attrs["name"] == "column_char"
    assert char_type_field.bytes_ is None

    nchar_type_field = struct.fields[10]
    assert isinstance(nchar_type_field, StringType)
    assert nchar_type_field.extra_attrs["name"] == "column_nchar"
    assert nchar_type_field.bytes_ is None

    date_type_field = struct.fields[11]
    assert isinstance(date_type_field, IntType)
    assert date_type_field.extra_attrs["name"] == "column_date"
    assert date_type_field.logical == "build.recap.Date"
    assert date_type_field.bits == 32
    assert date_type_field.signed is True
    assert date_type_field.extra_attrs["unit"] == "day"

    time_type_field = struct.fields[12]
    assert isinstance(time_type_field, IntType)
    assert time_type_field.extra_attrs["name"] == "column_time"
    assert time_type_field.logical == "build.recap.Time"
    assert time_type_field.bits == 32
    assert time_type_field.signed is True
    assert time_type_field.extra_attrs["unit"] == "microsecond"

    datetime_type_field = struct.fields[13]
    assert isinstance(datetime_type_field, IntType)
    assert datetime_type_field.extra_attrs["name"] == "column_datetime"
    assert datetime_type_field.logical == "build.recap.Timestamp"
    assert datetime_type_field.bits == 64
    assert datetime_type_field.signed is True
    assert datetime_type_field.extra_attrs["unit"] == "microsecond"
    assert datetime_type_field.extra_attrs["timezone"] == "UTC"

    timestamp_type_field = struct.fields[14]
    assert isinstance(timestamp_type_field, IntType)
    assert timestamp_type_field.extra_attrs["name"] == "column_timestamp"
    assert timestamp_type_field.logical == "build.recap.Timestamp"
    assert timestamp_type_field.bits == 64
    assert timestamp_type_field.signed is True
    assert timestamp_type_field.extra_attrs["unit"] == "microsecond"
    assert timestamp_type_field.extra_attrs["timezone"] == "UTC"

    binary_type_field = struct.fields[15]
    assert isinstance(binary_type_field, BytesType)
    assert binary_type_field.extra_attrs["name"] == "column_binary"
    assert binary_type_field.bytes_ is None

    decimal_type_field = struct.fields[16]
    assert isinstance(decimal_type_field, BytesType)
    assert decimal_type_field.extra_attrs["name"] == "column_decimal"
    assert decimal_type_field.bytes_ == 16
