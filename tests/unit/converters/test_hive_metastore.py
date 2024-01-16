import pytest
from pymetastore.htypes import (
    HCharType,
    HDecimalType,
    HListType,
    HMapType,
    HPrimitiveType,
    HStructType,
    HUnionType,
    HVarcharType,
    PrimitiveCategory,
)
from pymetastore.metastore import HColumn, HStorage, HTable, StorageFormat

from recap.converters.hive_metastore import HiveMetastoreConverter
from recap.types import (
    BoolType,
    BytesType,
    FloatType,
    IntType,
    ListType,
    MapType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


@pytest.mark.parametrize(
    "hive_type,expected",
    [
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.BOOLEAN),
            UnionType([NullType(), BoolType()], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.BYTE),
            UnionType([NullType(), IntType(bits=8)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.SHORT),
            UnionType([NullType(), IntType(bits=16)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.INT),
            UnionType([NullType(), IntType(bits=32)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.LONG),
            UnionType([NullType(), IntType(bits=64)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.FLOAT),
            UnionType([NullType(), FloatType(bits=32)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.DOUBLE),
            UnionType([NullType(), FloatType(bits=64)], default=None, name="test_col"),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.STRING),
            UnionType(
                [NullType(), StringType()],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.BINARY),
            UnionType(
                [NullType(), BytesType(bytes_=2_147_483_647)],
                default=None,
                name="test_col",
            ),
        ),
        (
            HDecimalType(precision=10, scale=2),
            UnionType(
                [
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=10,
                        scale=2,
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HVarcharType(length=255),
            UnionType(
                [NullType(), StringType(bytes_=255)],
                default=None,
                name="test_col",
            ),
        ),
        (
            HCharType(length=255),
            UnionType(
                [NullType(), StringType(bytes_=255, variable=False)],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.DATE),
            UnionType(
                [
                    NullType(),
                    IntType(
                        logical="build.recap.Date",
                        bits=32,
                        signed=True,
                        unit="day",
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.TIMESTAMP),
            UnionType(
                [
                    NullType(),
                    IntType(
                        logical="build.recap.Timestamp",
                        bits=64,
                        signed=True,
                        unit="nanosecond",
                        timezone="UTC",
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.TIMESTAMPLOCALTZ),
            UnionType(
                [
                    NullType(),
                    IntType(
                        logical="build.recap.Timestamp",
                        bits=64,
                        signed=True,
                        unit="nanosecond",
                        timezone=None,
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.INTERVAL_YEAR_MONTH),
            UnionType(
                [
                    NullType(),
                    BytesType(
                        logical="build.recap.Interval",
                        bytes_=12,
                        signed=True,
                        unit="month",
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HPrimitiveType(primitive_type=PrimitiveCategory.INTERVAL_DAY_TIME),
            UnionType(
                [
                    NullType(),
                    BytesType(
                        logical="build.recap.Interval",
                        bytes_=12,
                        signed=True,
                        unit="second",
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HMapType(
                key_type=HPrimitiveType(primitive_type=PrimitiveCategory.STRING),
                value_type=HPrimitiveType(primitive_type=PrimitiveCategory.INT),
            ),
            UnionType(
                [
                    NullType(),
                    MapType(
                        keys=UnionType(
                            [NullType(), StringType()],
                            default=None,
                        ),
                        values=UnionType(
                            [NullType(), IntType(bits=32)],
                            default=None,
                        ),
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HListType(
                element_type=HPrimitiveType(primitive_type=PrimitiveCategory.INT)
            ),
            UnionType(
                [
                    NullType(),
                    ListType(
                        values=UnionType(
                            [NullType(), IntType(bits=32)],
                            default=None,
                        ),
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HUnionType(
                types=[
                    HPrimitiveType(primitive_type=PrimitiveCategory.INT),
                    HPrimitiveType(primitive_type=PrimitiveCategory.STRING),
                ]
            ),
            UnionType(
                [
                    NullType(),
                    IntType(bits=32),
                    StringType(),
                ],
                default=None,
                name="test_col",
            ),
        ),
        (
            HStructType(
                names=["field1", "field2"],
                types=[
                    HPrimitiveType(primitive_type=PrimitiveCategory.INT),
                    HPrimitiveType(primitive_type=PrimitiveCategory.STRING),
                ],
            ),
            UnionType(
                [
                    NullType(),
                    StructType(
                        fields=[
                            UnionType(
                                [NullType(), IntType(bits=32)],
                                default=None,
                                name="field1",
                            ),
                            UnionType(
                                [
                                    NullType(),
                                    StringType(),
                                ],
                                default=None,
                                name="field2",
                            ),
                        ]
                    ),
                ],
                default=None,
                name="test_col",
            ),
        ),
    ],
)
def test_hive_metastore_converter(hive_type, expected):
    table = HTable(
        database_name="test_db",
        name="test_table",
        table_type="EXTERNAL_TABLE",
        columns=[HColumn(name="test_col", type=hive_type)],
        partition_columns=[],
        storage=HStorage(StorageFormat("", "", "")),
        parameters={},
    )

    result = HiveMetastoreConverter().to_recap(table)

    assert result.fields[0] == expected
