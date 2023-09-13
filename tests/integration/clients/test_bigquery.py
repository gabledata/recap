import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

from recap.clients.bigquery import BigQueryClient
from recap.types import (
    BoolType,
    BytesType,
    FloatType,
    IntType,
    ListType,
    NullType,
    StringType,
    StructType,
    UnionType,
)


@pytest.fixture(scope="module")
def client():
    client_options = ClientOptions(api_endpoint="http://localhost:9050")
    client = bigquery.Client(
        "test_project",
        client_options=client_options,
        credentials=AnonymousCredentials(),
    )
    return client


@pytest.fixture(scope="module", autouse=True)
def setup_data(client):
    # Creating a table with all primitive types.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table` (
            test_string STRING,
            test_bytes BYTES,
            test_int64 INT64,
            test_float64 FLOAT64,
            test_boolean BOOLEAN,
            test_timestamp TIMESTAMP,
            test_datetime DATETIME,
            test_date DATE,
            test_time TIME,
            test_numeric NUMERIC,
            test_bigdecimal BIGNUMERIC
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with REQUIRED primitive types.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_required` (
            test_string STRING NOT NULL,
            test_bytes BYTES NOT NULL,
            test_int64 INT64 NOT NULL,
            test_float64 FLOAT64 NOT NULL,
            test_boolean BOOLEAN NOT NULL,
            test_timestamp TIMESTAMP NOT NULL,
            test_datetime DATETIME NOT NULL,
            test_date DATE NOT NULL,
            test_time TIME NOT NULL,
            test_numeric NUMERIC NOT NULL,
            test_bigdecimal BIGNUMERIC NOT NULL
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with a nested STRUCT / RECORD type.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_struct` (
            test_record STRUCT<
                test_boolean BOOLEAN,
                nested_record STRUCT<
                    test_nested_boolean BOOLEAN
                >
            >
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with a repeated (array) type.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_repeated` (
            test_array ARRAY<STRING>
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with a repeated RECORD type.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_repeated_records` (
            test_array ARRAY<STRUCT<test_boolean BOOLEAN, test_int64 INT64>>
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with a default value.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_default` (
            test_string STRING DEFAULT "default_value"
        )
        """,
        job_config=QueryJobConfig(),
    )

    # Creating a table with a column description.
    client.query(
        query="""
        CREATE TABLE `test_dataset.test_table_description` (
            test_string STRING OPTIONS(description="This is a test string column")
        )
        """,
        job_config=QueryJobConfig(),
    )


def test_primitive_types(client):
    client = BigQueryClient(client)
    recap_schema = client.schema("test_project", "test_dataset", "test_table")
    recap_fields = recap_schema.fields

    assert recap_fields[0] == UnionType(
        types=[NullType(), StringType()],
        default=None,
        name="test_string",
    )

    assert recap_fields[1] == UnionType(
        types=[NullType(), BytesType()],
        default=None,
        name="test_bytes",
    )

    assert recap_fields[2] == UnionType(
        types=[NullType(), IntType(bits=64)],
        default=None,
        name="test_int64",
    )

    assert recap_fields[3] == UnionType(
        types=[NullType(), FloatType(bits=64)],
        default=None,
        name="test_float64",
    )

    assert recap_fields[4] == UnionType(
        types=[NullType(), BoolType()],
        default=None,
        name="test_boolean",
    )

    assert recap_fields[5] == UnionType(
        types=[
            NullType(),
            IntType(
                bits=64,
                logical="build.recap.Timestamp",
                unit="microsecond",
            ),
        ],
        default=None,
        name="test_timestamp",
    )

    assert recap_fields[6] == UnionType(
        types=[
            NullType(),
            IntType(
                bits=64,
                logical="build.recap.Timestamp",
                unit="microsecond",
            ),
        ],
        default=None,
        name="test_datetime",
    )

    assert recap_fields[7] == UnionType(
        types=[
            NullType(),
            IntType(
                bits=32,
                logical="build.recap.Date",
                unit="day",
            ),
        ],
        default=None,
        name="test_date",
    )

    assert recap_fields[8] == UnionType(
        types=[
            NullType(),
            IntType(
                bits=32,
                logical="build.recap.Time",
                unit="microsecond",
            ),
        ],
        default=None,
        name="test_time",
    )

    assert recap_fields[9] == UnionType(
        types=[
            NullType(),
            BytesType(
                bytes_=16,
                variable=False,
                logical="build.recap.Decimal",
                precision=38,
                scale=0,
            ),
        ],
        default=None,
        name="test_numeric",
    )

    assert recap_fields[10] == UnionType(
        types=[
            NullType(),
            BytesType(
                bytes_=32,
                variable=False,
                logical="build.recap.Decimal",
                precision=76,
                scale=0,
            ),
        ],
        default=None,
        name="test_bigdecimal",
    )


# TODO Remove xfail after https://github.com/goccy/bigquery-emulator/issues/210
@pytest.mark.xfail(reason="BigQuery emulator does not support REQUIRED fields")
def test_required_types(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_required",
    )
    recap_fields = recap_schema.fields

    assert recap_fields[0] == StringType(name="test_string")

    assert recap_fields[1] == BytesType(name="test_bytes")

    assert recap_fields[2] == IntType(bits=64, name="test_int64")

    assert recap_fields[3] == FloatType(bits=64, name="test_float64")

    assert recap_fields[4] == BoolType(name="test_boolean")

    assert recap_fields[5] == IntType(
        bits=64,
        logical="build.recap.Timestamp",
        unit="microsecond",
        name="test_timestamp",
    )

    assert recap_fields[6] == IntType(
        bits=64,
        logical="build.recap.Timestamp",
        unit="microsecond",
        name="test_datetime",
    )

    assert recap_fields[7] == IntType(
        bits=32,
        logical="build.recap.Date",
        unit="day",
        name="test_date",
    )

    assert recap_fields[8] == IntType(
        bits=32,
        logical="build.recap.Time",
        unit="microsecond",
        name="test_time",
    )

    assert recap_fields[9] == BytesType(
        bytes_=16,
        variable=False,
        logical="build.recap.Decimal",
        precision=38,
        scale=0,
        name="test_numeric",
    )

    assert recap_fields[10] == BytesType(
        bytes_=32,
        variable=False,
        logical="build.recap.Decimal",
        precision=76,
        scale=0,
        name="test_bigdecimal",
    )


def test_nested_struct_record_types(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_struct",
    )
    recap_fields = recap_schema.fields

    assert recap_fields[0] == UnionType(
        types=[
            NullType(),
            StructType(
                fields=[
                    UnionType(
                        types=[
                            NullType(),
                            BoolType(),
                        ],
                        default=None,
                        name="test_boolean",
                    ),
                    UnionType(
                        types=[
                            NullType(),
                            StructType(
                                fields=[
                                    UnionType(
                                        types=[
                                            NullType(),
                                            BoolType(),
                                        ],
                                        default=None,
                                        name="test_nested_boolean",
                                    ),
                                ],
                            ),
                        ],
                        default=None,
                        name="nested_record",
                    ),
                ],
            ),
        ],
        default=None,
        name="test_record",
    )


def test_repeated_types(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_repeated",
    )
    recap_fields = recap_schema.fields

    # ARRAYs can't be nullable and their elements can't be nullable. See:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_nulls

    assert recap_fields[0] == ListType(
        values=StringType(),
        name="test_array",
    )


def test_repeated_records(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_repeated_records",
    )
    recap_fields = recap_schema.fields

    assert recap_fields[0] == ListType(
        values=StructType(
            fields=[
                UnionType(
                    types=[
                        NullType(),
                        BoolType(),
                    ],
                    default=None,
                    name="test_boolean",
                ),
                UnionType(
                    types=[
                        NullType(),
                        IntType(bits=64),
                    ],
                    default=None,
                    name="test_int64",
                ),
            ],
        ),
        name="test_array",
    )


# TODO Remove xfail after https://github.com/goccy/bigquery-emulator/issues/211
@pytest.mark.xfail(reason="BigQuery emulator does not support DEFAULT values")
def test_default_value(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_default",
    )
    recap_fields = recap_schema.fields

    assert recap_fields[0] == UnionType(
        types=[NullType(), StringType()],
        default="default_value",
        name="test_string",
    )


# TODO Remove xfail after https://github.com/goccy/bigquery-emulator/issues/212
@pytest.mark.xfail(reason="BigQuery emulator does not support OPTIONS")
def test_column_description(client):
    client = BigQueryClient(client)
    recap_schema = client.schema(
        "test_project",
        "test_dataset",
        "test_table_description",
    )
    recap_fields = recap_schema.fields

    assert recap_fields[0] == UnionType(
        types=[NullType(), StringType()],
        default=None,
        name="test_string",
        docs="This is a test string column",
    )


def test_ls(client):
    client = BigQueryClient(client)
    # TODO BigQuery emulator doesn't support ls_projects()
    # assert client.ls() == ["test_project"]
    assert client.ls("test_project") == ["test_dataset"]
    assert client.ls("test_project", "test_dataset") == [
        "test_table",
        "test_table_required",
        "test_table_struct",
        "test_table_repeated",
        "test_table_repeated_records",
        # TODO Enable after https://github.com/goccy/bigquery-emulator/issues/211
        # "test_table_default",
        "test_table_description",
    ]
