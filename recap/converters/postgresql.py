from math import ceil
from typing import Any

from recap.converters.dbapi import DbapiConverter
from recap.types import BoolType, BytesType, FloatType, IntType, RecapType, StringType

MAX_FIELD_SIZE = 1073741824


class PostgresqlConverter(DbapiConverter):
    def _parse_type(self, column_props: dict[str, Any]) -> RecapType:
        data_type = column_props["DATA_TYPE"].lower()
        octet_length = column_props["CHARACTER_OCTET_LENGTH"]
        max_length = column_props["CHARACTER_MAXIMUM_LENGTH"]

        if data_type in ["bigint", "int8", "bigserial", "serial8"]:
            base_type = IntType(bits=64, signed=True)
        elif data_type in ["integer", "int", "int4", "serial", "serial4"]:
            base_type = IntType(bits=32, signed=True)
        elif data_type in ["smallint", "smallserial", "serial2"]:
            base_type = IntType(bits=16, signed=True)
        elif data_type in ["double precision", "float8"]:
            base_type = FloatType(bits=64)
        elif data_type in ["real", "float4"]:
            base_type = FloatType(bits=32)
        elif data_type == "boolean":
            base_type = BoolType()
        elif (
            data_type in ["text", "json", "jsonb"]
            or data_type.startswith("character varying")
            or data_type.startswith("varchar")
        ):
            base_type = StringType(bytes_=octet_length, variable=True)
        elif data_type.startswith("char"):
            base_type = StringType(bytes_=octet_length, variable=False)
        elif data_type == "uuid":
            base_type = StringType(
                logical="build.recap.UUID",
                bytes_=36,
                variable=False,
            )
        elif data_type == "bytea" or data_type.startswith("bit varying"):
            base_type = BytesType(bytes_=MAX_FIELD_SIZE, variable=True)
        elif data_type.startswith("bit"):
            byte_length = ceil(max_length / 8)
            base_type = BytesType(bytes_=byte_length, variable=False)
        elif data_type.startswith("timestamp"):
            dt_precision = column_props["DATETIME_PRECISION"]
            unit = self._get_time_unit([dt_precision]) or "microsecond"
            base_type = IntType(
                bits=64,
                logical="build.recap.Timestamp",
                unit=unit,
            )
        elif data_type.startswith("decimal") or data_type.startswith("numeric"):
            base_type = BytesType(
                logical="build.recap.Decimal",
                bytes_=32,
                variable=False,
                precision=column_props["NUMERIC_PRECISION"],
                scale=column_props["NUMERIC_SCALE"],
            )
        else:
            raise ValueError(f"Unknown data type: {data_type}")

        return base_type
