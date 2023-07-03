from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, List, Protocol, Tuple

from recap.types import RecapType, StructType


class DbapiReader(ABC):
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def to_recap(self, table: str, schema: str, catalog: str) -> StructType:
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
                SELECT
                    *
                FROM information_schema.columns
                WHERE table_name = {self.param_style}
                    AND table_schema = {self.param_style}
                    AND table_catalog = {self.param_style}
                ORDER BY ordinal_position ASC
            """,
            (table, schema, catalog),
        )

        names = [name[0].upper() for name in cursor.description]
        fields = []
        for row in cursor.fetchall():
            column_props = dict(zip(names, row))
            base_type = self.get_recap_type(column_props)
            is_nullable = column_props["IS_NULLABLE"].upper() == "YES"

            if is_nullable:
                base_type = base_type.make_nullable()

            if column_props["COLUMN_DEFAULT"] is not None or is_nullable:
                # TODO Convert default value to the correct type, not just a string
                base_type.extra_attrs["default"] = column_props["COLUMN_DEFAULT"]

            base_type.extra_attrs["name"] = column_props["COLUMN_NAME"]

            fields.append(base_type)

        return StructType(fields=fields)

    @property
    def param_style(cls) -> str:
        return "%s"

    @abstractmethod
    def get_recap_type(self, column_props: dict[str, Any]) -> RecapType:
        ...

    def _get_time_unit(self, params: list[str] | None) -> str | None:
        match params:
            case [unit, _] if int(unit) == 0:
                return "second"
            case [unit, _] if int(unit) <= 3:
                return "millisecond"
            case [unit, _] if int(unit) <= 6:
                return "microsecond"

    def _parse_parameters(self, col_type: str) -> list[str] | None:
        """
        Parse types that have parameters.
        """
        match = re.search(r"\((.*?)\)", col_type)
        if match:
            return [p.strip() for p in match.group(1).split(",")]
        return None


class Connection(Protocol):
    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def cursor(self) -> Cursor:
        ...


class Cursor(Protocol):
    def execute(self, query: str, parameters: Tuple = ()) -> None:
        ...

    def executemany(self, query: str, parameter_list: List[Tuple]) -> None:
        ...

    def fetchone(self) -> Tuple:
        ...

    def fetchall(self) -> List[Tuple]:
        ...

    def fetchmany(self, size: int) -> List[Tuple]:
        ...

    @property
    def description(self) -> List[Tuple]:
        ...
