import psycopg2

from recap.clients import create_client
from recap.clients.postgresql import PostgresqlClient
from recap.converters.postgresql import MAX_FIELD_SIZE
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


class TestPostgresqlClient:
    @classmethod
    def setup_class(cls):
        # Connect to the PostgreSQL database
        cls.connection = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="password",
            dbname="testdb",
        )

        # Create tables
        cursor = cls.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_types (
                test_bigint BIGINT,
                test_integer INTEGER,
                test_smallint SMALLINT,
                test_float DOUBLE PRECISION,
                test_real REAL,
                test_boolean BOOLEAN,
                test_text TEXT,
                test_char CHAR(10),
                test_bytea BYTEA,
                test_bit BIT(10),
                test_timestamp TIMESTAMP,
                test_decimal DECIMAL(10,2),
                test_not_null INTEGER NOT NULL,
                test_not_null_default INTEGER NOT NULL DEFAULT 1,
                test_default INTEGER DEFAULT 2
            );
        """
        )
        cls.connection.commit()

    @classmethod
    def teardown_class(cls):
        # Delete tables
        cursor = cls.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_types;")
        cls.connection.commit()

        # Close the connection
        cls.connection.close()

    def test_struct_method(self):
        # Initiate the PostgresqlClient class
        client = PostgresqlClient(self.connection)  # type: ignore

        # Test 'test_types' table
        test_types_struct = client.schema("testdb", "public", "test_types")

        # Define the expected output for 'test_types' table
        expected_fields = [
            UnionType(
                default=None,
                name="test_bigint",
                types=[NullType(), IntType(bits=64, signed=True)],
            ),
            UnionType(
                default=None,
                name="test_integer",
                types=[NullType(), IntType(bits=32, signed=True)],
            ),
            UnionType(
                default=None,
                name="test_smallint",
                types=[NullType(), IntType(bits=16, signed=True)],
            ),
            UnionType(
                default=None,
                name="test_float",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="test_real",
                types=[NullType(), FloatType(bits=32)],
            ),
            UnionType(
                default=None,
                name="test_boolean",
                types=[NullType(), BoolType()],
            ),
            UnionType(
                default=None,
                name="test_text",
                types=[NullType(), StringType(bytes_=MAX_FIELD_SIZE, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_char",
                # 40 = max of 4 bytes in a UTF-8 encoded unicode character * 10 chars
                types=[NullType(), StringType(bytes_=40, variable=False)],
            ),
            UnionType(
                default=None,
                name="test_bytea",
                types=[NullType(), BytesType(bytes_=MAX_FIELD_SIZE, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_bit",
                types=[NullType(), BytesType(bytes_=2, variable=False)],
            ),
            UnionType(
                default=None,
                name="test_timestamp",
                types=[
                    NullType(),
                    IntType(
                        bits=64, logical="build.recap.Timestamp", unit="microsecond"
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="test_decimal",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=32,
                        variable=False,
                        precision=10,
                        scale=2,
                    ),
                ],
            ),
            IntType(bits=32, signed=True, name="test_not_null"),
            IntType(bits=32, signed=True, name="test_not_null_default", default="1"),
            UnionType(
                default="2",
                name="test_default",
                types=[NullType(), IntType(bits=32, signed=True)],
            ),
        ]

        # Going field by field to make debugging easier when test fails
        for field, expected_field in zip(test_types_struct.fields, expected_fields):
            assert field == expected_field

        assert test_types_struct == StructType(fields=expected_fields)

    def test_create_client(self):
        postgresql_url = "postgresql://postgres:password@localhost:5432/testdb"

        with create_client(postgresql_url) as client:
            assert client.ls() == ["postgres", "template0", "template1", "testdb"]
            assert client.ls("testdb") == [
                "pg_toast",
                "pg_catalog",
                "public",
                "information_schema",
            ]
            assert client.ls("testdb", "public") == ["test_types"]
