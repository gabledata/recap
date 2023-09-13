from unittest.mock import MagicMock, patch

import fakesnow
import pytest
import snowflake.connector

from recap.clients.snowflake import SnowflakeClient
from recap.types import (
    BoolType,
    BytesType,
    FloatType,
    IntType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


class TestSnowflakeClient:
    @classmethod
    def setup_class(cls):
        with fakesnow.patch():
            cls.connection = snowflake.connector.connect()
            cursor = cls.connection.cursor()
            cursor.execute("CREATE OR REPLACE DATABASE testdb;")
            cursor.execute("USE DATABASE testdb;")
            cursor.execute("CREATE OR REPLACE SCHEMA public;")
            cursor.execute("USE SCHEMA testdb.public;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS test_types (
                    TEST_BIGINT BIGINT,
                    TEST_INTEGER INTEGER,
                    TEST_SMALLINT SMALLINT,
                    TEST_FLOAT FLOAT,
                    TEST_FLOAT8 FLOAT8,
                    TEST_FLOAT4 FLOAT4,
                    TEST_DOUBLE FLOAT,
                    TEST_DOUBLE_PRECISION DOUBLE PRECISION,
                    TEST_REAL REAL,
                    TEST_BOOLEAN BOOLEAN,
                    TEST_VARCHAR VARCHAR(100),
                    TEST_STRING STRING,
                    TEST_TEXT TEXT,
                    TEST_CHAR CHAR(10),
                    TEST_NVARCHAR NVARCHAR(100),
                    TEST_NVARCHAR2 NVARCHAR(100),
                    TEST_CHAR_VARYING CHAR VARYING(100),
                    TEST_NCHAR_VARYING NCHAR VARYING(100),
                    TEST_NCHAR NCHAR(100),
                    TEST_CHARACTER CHARACTER(100),
                    TEST_BINARY BINARY,
                    TEST_VARBINARY VARBINARY,
                    TEST_BLOB BLOB,
                    TEST_DATE DATE,
                    TEST_TIMESTAMP TIMESTAMP,
                    TEST_DATETIME DATETIME,
                    TEST_TIME TIME,
                    TEST_DECIMAL DECIMAL(10,2),
                    TEST_NUMERIC NUMERIC(10,2),
                    TEST_NUMBER NUMBER(10,2),
                    TEST_TINYINT TINYINT,
                    TEST_BYTEINT BYTEINT
                );
                """
            )

    def test_struct_method(self):
        client = SnowflakeClient(self.connection)  # type: ignore
        test_types_struct = client.schema("TESTDB", "PUBLIC", "TEST_TYPES")
        expected_fields = [
            UnionType(
                default=None,
                name="TEST_BIGINT",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=38,
                        scale=0,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_INTEGER",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=38,
                        scale=0,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_SMALLINT",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=38,
                        scale=0,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_FLOAT",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_FLOAT8",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_FLOAT4",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_DOUBLE",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_DOUBLE_PRECISION",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_REAL",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="TEST_BOOLEAN",
                types=[NullType(), BoolType()],
            ),
            UnionType(
                default=None,
                name="TEST_VARCHAR",
                # 100 * 4 bytes per character = 400 bytes
                types=[NullType(), StringType(bytes_=400, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_STRING",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_TEXT",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_CHAR",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_NVARCHAR",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_NVARCHAR2",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_CHAR_VARYING",
                # 100 * 4 bytes per character = 400 bytes
                types=[NullType(), StringType(bytes_=400, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_NCHAR_VARYING",
                # 100 * 4 bytes per character = 400 bytes
                types=[NullType(), StringType(bytes_=400, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_NCHAR",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_CHARACTER",
                types=[NullType(), StringType(bytes_=16_777_216, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_BINARY",
                types=[NullType(), BytesType(bytes_=8_388_608, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_VARBINARY",
                types=[NullType(), BytesType(bytes_=8_388_608, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_BLOB",
                types=[NullType(), BytesType(bytes_=8_388_608, variable=True)],
            ),
            UnionType(
                default=None,
                name="TEST_DATE",
                types=[
                    NullType(),
                    IntType(bits=32, logical="build.recap.Date", unit="day"),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_TIMESTAMP",
                types=[
                    NullType(),
                    IntType(
                        bits=64,
                        logical="build.recap.Timestamp",
                        unit="nanosecond",
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_DATETIME",
                types=[
                    NullType(),
                    IntType(
                        bits=64,
                        logical="build.recap.Timestamp",
                        unit="nanosecond",
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_TIME",
                types=[
                    NullType(),
                    IntType(bits=32, logical="build.recap.Time", unit="nanosecond"),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_DECIMAL",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=10,
                        scale=2,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_NUMERIC",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=10,
                        scale=2,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_NUMBER",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=10,
                        scale=2,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_TINYINT",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=38,
                        scale=0,
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="TEST_BYTEINT",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=38,
                        scale=0,
                    ),
                ],
            ),
        ]

        assert test_types_struct == StructType(fields=expected_fields)  # type: ignore

    def test_ls(self):
        client = SnowflakeClient(self.connection)  # type: ignore
        assert client.ls() == ["TESTDB"]
        assert client.ls("TESTDB") == [
            "PUBLIC",
            "information_schema",
            "main",
            "pg_catalog",
        ]
        assert client.ls("TESTDB", "PUBLIC") == ["TEST_TYPES"]

    def test_snowflake_client_create(self):
        with patch("snowflake.connector.connect") as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_connection

            with SnowflakeClient.create(
                host="some_account",
                paths=("some_database", "some_schema"),
            ) as _:
                mock_connect.assert_called_once_with(
                    account="some_account",
                    database="some_database",
                    schema="some_schema",
                    paths=("some_database", "some_schema"),
                )

    @pytest.mark.parametrize(
        "method, url_args, expected_result",
        [
            # Test "ls" method with only URL (no catalog or schema)
            (
                "ls",
                {"scheme": "snowflake", "netloc": "snowflake-url"},
                ("snowflake://snowflake-url", [None, None]),
            ),
            # Test "ls" method with URL and catalog
            (
                "ls",
                {"scheme": "snowflake", "netloc": "snowflake-url", "paths": ["cat1"]},
                ("snowflake://snowflake-url/cat1", ["cat1", None]),
            ),
            # Test "ls" method with URL, catalog, and schema
            (
                "ls",
                {
                    "scheme": "snowflake",
                    "netloc": "snowflake-url",
                    "paths": ["cat1", "sch1"],
                },
                ("snowflake://snowflake-url/cat1", ["cat1", "sch1"]),
            ),
            # Test "schema" method with URL, catalog, schema, and table
            (
                "schema",
                {
                    "scheme": "snowflake",
                    "netloc": "snowflake-url",
                    "paths": ["cat1", "sch1", "tbl1"],
                },
                ("snowflake://snowflake-url/cat1", ["cat1", "sch1", "tbl1"]),
            ),
            # Test invalid method
            (
                "invalid_method",
                {"scheme": "snowflake", "netloc": "snowflake-url"},
                pytest.raises(ValueError, match="Invalid method"),
            ),
            # Test "schema" method with insufficient paths (only catalog and schema, no table)
            (
                "schema",
                {
                    "scheme": "snowflake",
                    "netloc": "snowflake-url",
                    "paths": ["cat1", "sch1"],
                },
                pytest.raises(ValueError, match="Invalid method"),
            ),
            # Test "ls" method with all paths (catalog, schema, and table)
            (
                "ls",
                {
                    "scheme": "snowflake",
                    "netloc": "snowflake-url",
                    "paths": ["cat1", "sch1", "tbl1"],
                },
                pytest.raises(ValueError, match="Invalid method"),
            ),
        ],
    )
    def test_parse_method(self, method, url_args, expected_result):
        if isinstance(expected_result, tuple):
            result = SnowflakeClient.parse(method, **url_args)
            assert result == expected_result
        else:
            with expected_result:
                SnowflakeClient.parse(method, **url_args)
