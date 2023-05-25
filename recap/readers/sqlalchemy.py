from sqlalchemy import create_engine, engine, inspect, types

from recap.types import BoolType, BytesType, FloatType, IntType, StringType, StructType


class SqlAlchemyReader:
    def __init__(self, connection: str | engine.Engine):
        self.engine = (
            connection
            if isinstance(connection, engine.Engine)
            else create_engine(connection)
        )
        self.inspector = inspect(self.engine)

    def struct(self, table: str) -> StructType:
        columns = self.inspector.get_columns(table)
        fields = []
        for column in columns:
            field = None
            match column["type"]:
                case types.INTEGER():
                    field = IntType(bits=32, signed=True, name=column["name"])
                case types.BIGINT():
                    field = IntType(bits=64, signed=True, name=column["name"])
                case types.SMALLINT():
                    field = IntType(bits=16, signed=True, name=column["name"])
                case types.FLOAT():
                    field = FloatType(bits=64, name=column["name"])
                case types.REAL():
                    field = FloatType(bits=32, name=column["name"])
                case types.BOOLEAN():
                    field = BoolType(name=column["name"])
                case types.VARCHAR() | types.TEXT() | types.NVARCHAR() | types.CLOB():
                    field = StringType(
                        bytes_=column["type"].length, name=column["name"]
                    )
                case types.CHAR() | types.NCHAR():
                    field = StringType(
                        bytes_=column["type"].length,
                        variable=False,
                        name=column["name"],
                    )
                case types.DATE():
                    field = IntType(
                        logical="build.recap.Date",
                        bits=32,
                        signed=True,
                        unit="day",
                        name=column["name"],
                    )
                case types.TIME():
                    field = IntType(
                        logical="build.recap.Time",
                        bits=32,
                        signed=True,
                        unit="microsecond",
                        name=column["name"],
                    )
                case types.DATETIME() | types.TIMESTAMP():
                    field = IntType(
                        logical="build.recap.Timestamp",
                        bits=64,
                        signed=True,
                        unit="microsecond",
                        timezone="UTC",
                        name=column["name"],
                    )
                case types.BINARY() | types.VARBINARY() | types.BLOB():
                    is_variable = not isinstance(column["type"], types.BINARY)
                    field = BytesType(
                        bytes_=column["type"].length,
                        variable=is_variable,
                        name=column["name"],
                    )
                case types.DECIMAL() | types.NUMERIC():
                    field = BytesType(
                        logical="build.recap.Decimal",
                        bytes_=16,
                        variable=False,
                        precision=column["type"].precision,
                        scale=column["type"].scale,
                        name=column["name"],
                    )
                case _:
                    raise TypeError(f"Unsupported type {column['type']}")

            fields.append(field)

        return StructType(fields=fields, name=table)
