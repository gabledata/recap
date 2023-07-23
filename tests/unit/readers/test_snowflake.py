import fakesnow
import snowflake.connector

from recap.readers.snowflake import SnowflakeReader
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


class TestSnowflakeReader:
    @classmethod
    def setup_class(cls):
        with fakesnow.patch():
            # Connect to the PostgreSQL database
            cls.connection = snowflake.connector.connect()

            # Create tables
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
        # Initiate the SnowflakeReader class
        reader = SnowflakeReader(self.connection)  # type: ignore

        # Test 'test_types' table
        test_types_struct = reader.to_recap("TEST_TYPES", "PUBLIC", "TESTDB")

        # Define the expected output for 'test_types' table
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
