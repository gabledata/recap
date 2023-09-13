import mysql.connector

from recap.clients import create_client
from recap.clients.mysql import MysqlClient
from recap.types import (
    BytesType,
    FloatType,
    IntType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


class TestMySqlClient:
    @classmethod
    def setup_class(cls):
        # Connect to the MySQL database
        cls.connection = mysql.connector.connect(
            database="testdb",
            user="mysql",
            password="password",
            host="localhost",
            port=3306,
        )

        # Create tables
        cursor = cls.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS test_types (
                test_bigint BIGINT,
                test_integer INTEGER,
                test_smallint SMALLINT,
                test_float FLOAT,
                test_double DOUBLE PRECISION,
                test_real REAL,
                test_boolean BOOLEAN,
                test_text TEXT,
                test_char CHAR(10),
                test_blob BLOB,
                test_mediumblob MEDIUMBLOB,
                test_longblob LONGBLOB,
                test_mediumtext MEDIUMTEXT,
                test_longtext LONGTEXT,
                test_tinyblob TINYBLOB,
                test_tinytext TINYTEXT,
                test_varchar VARCHAR(255),
                test_varbinary VARBINARY(255),
                test_binary BINARY(255),
                test_bit BIT(10),
                test_timestamp TIMESTAMP,
                test_timestamp_millis TIMESTAMP(3),
                test_decimal DECIMAL(10,2),
                test_numeric NUMERIC(11,3),
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
        # Initiate the MySQL client class
        client = MysqlClient(self.connection)  # type: ignore

        # Test 'test_types' table. MySQL catalog is always 'def'.
        test_types_struct = client.schema("testdb", "test_types")

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
                types=[NullType(), FloatType(bits=32)],
            ),
            UnionType(
                default=None,
                name="test_double",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="test_real",
                types=[NullType(), FloatType(bits=64)],
            ),
            UnionType(
                default=None,
                name="test_boolean",
                types=[NullType(), IntType(bits=8, signed=True)],
            ),
            UnionType(
                default=None,
                name="test_text",
                types=[NullType(), StringType(bytes_=65_535, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_char",
                # 40 = max of 4 bytes in a UTF-8 encoded unicode character * 10 chars
                types=[NullType(), StringType(bytes_=40, variable=False)],
            ),
            UnionType(
                default=None,
                name="test_blob",
                types=[NullType(), BytesType(bytes_=65_535, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_mediumblob",
                types=[NullType(), BytesType(bytes_=16_777_215, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_longblob",
                types=[NullType(), BytesType(bytes_=4_294_967_295, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_mediumtext",
                types=[NullType(), StringType(bytes_=16_777_215, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_longtext",
                types=[NullType(), StringType(bytes_=4_294_967_295, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_tinyblob",
                types=[NullType(), BytesType(bytes_=255, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_tinytext",
                types=[NullType(), StringType(bytes_=255, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_varchar",
                types=[
                    NullType(),
                    StringType(bytes_=1020, variable=True),
                ],  # max 4 bytes per char * 255
            ),
            UnionType(
                default=None,
                name="test_varbinary",
                types=[NullType(), BytesType(bytes_=255, variable=True)],
            ),
            UnionType(
                default=None,
                name="test_binary",
                types=[NullType(), BytesType(bytes_=255, variable=False)],
            ),
            UnionType(
                default=None,
                name="test_bit",
                types=[NullType(), BytesType(bytes_=8, variable=False)],
            ),
            UnionType(
                default=None,
                name="test_timestamp",
                types=[
                    NullType(),
                    IntType(
                        bits=64,
                        logical="build.recap.Timestamp",
                        unit="second",
                    ),
                ],
            ),
            UnionType(
                default=None,
                name="test_timestamp_millis",
                types=[
                    NullType(),
                    IntType(
                        bits=64,
                        logical="build.recap.Timestamp",
                        unit="millisecond",
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
            UnionType(
                default=None,
                name="test_numeric",
                types=[
                    NullType(),
                    BytesType(
                        logical="build.recap.Decimal",
                        bytes_=32,
                        variable=False,
                        precision=11,
                        scale=3,
                    ),
                ],
            ),
            IntType(bits=32, signed=True, name="test_not_null"),
            IntType(bits=32, signed=True, name="test_not_null_default", default=b"1"),
            UnionType(
                default=b"2",
                name="test_default",
                types=[NullType(), IntType(bits=32, signed=True)],
            ),
        ]

        # Going field by field to make debugging easier when test fails
        for field, expected_field in zip(test_types_struct.fields, expected_fields):
            assert field == expected_field

        assert test_types_struct == StructType(fields=expected_fields)

    def test_create_client(self):
        mysql_url = "mysql://mysql:password@localhost:3306/testdb"

        with create_client(mysql_url) as client:
            assert client.ls() == ["information_schema", "performance_schema", "testdb"]
            assert client.ls("testdb") == ["test_types"]
