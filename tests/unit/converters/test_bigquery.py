import pytest
from google.cloud import bigquery

from recap.converters.bigquery import BigQueryConverter
from recap.types import BoolType, BytesType, FloatType, IntType, StringType, StructType


@pytest.mark.parametrize(
    "field_type,expected",
    [
        ("STRING", StringType(name="test_field")),
        ("BYTES", BytesType(name="test_field")),
        ("INT64", IntType(bits=64, name="test_field")),
        ("FLOAT", FloatType(bits=64, name="test_field")),
        ("BOOLEAN", BoolType(name="test_field")),
        (
            "TIMESTAMP",
            IntType(
                logical="build.recap.Timestamp",
                bits=64,
                unit="microsecond",
                name="test_field",
            ),
        ),
        (
            "TIME",
            IntType(
                logical="build.recap.Time",
                bits=32,
                unit="microsecond",
                name="test_field",
            ),
        ),
        (
            "DATE",
            IntType(logical="build.recap.Date", bits=32, unit="day", name="test_field"),
        ),
        (
            "NUMERIC",
            BytesType(
                logical="build.recap.Decimal",
                bytes_=16,
                variable=False,
                precision=38,
                scale=0,
                name="test_field",
            ),
        ),
        (
            "BIGNUMERIC",
            BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=76,
                scale=0,
                name="test_field",
            ),
        ),
    ],
)
def test_all_cases(field_type, expected):
    field = bigquery.SchemaField(
        name="test_field", field_type=field_type, mode="REQUIRED"
    )
    result = BigQueryConverter().to_recap([field])
    assert result.fields[0] == expected


def test_record():
    nested_fields = [
        bigquery.SchemaField(name="nested_int", field_type="INT64", mode="REQUIRED"),
        bigquery.SchemaField(
            name="nested_string", field_type="STRING", mode="REQUIRED"
        ),
    ]

    field = bigquery.SchemaField(
        name="test_record", field_type="RECORD", mode="REQUIRED", fields=nested_fields
    )
    expected = StructType(
        [
            IntType(bits=64, name="nested_int"),
            StringType(bytes_=None, name="nested_string"),
        ],
        name="test_record",
    )

    result = BigQueryConverter().to_recap([field])
    assert result.fields[0] == expected


def test_unrecognized():
    with pytest.raises(ValueError):
        field = bigquery.SchemaField(
            name="test_field", field_type="UNKNOWN", mode="REQUIRED"
        )
        BigQueryConverter().to_recap([field])
