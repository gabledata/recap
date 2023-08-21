import re
from abc import ABC, abstractmethod
from typing import Any

from recap.types import RecapType, StructType


class DbapiConverter(ABC):
    def to_recap(self, columns: list[dict[str, Any]]) -> StructType:
        fields = []
        for column in columns:
            base_type = self._parse_type(column)
            is_nullable = column["IS_NULLABLE"].upper() == "YES"

            if is_nullable:
                base_type = base_type.make_nullable()

            if column["COLUMN_DEFAULT"] is not None or is_nullable:
                # TODO Convert default value to the correct type, not just a string
                base_type.extra_attrs["default"] = column["COLUMN_DEFAULT"]

            base_type.extra_attrs["name"] = column["COLUMN_NAME"]

            fields.append(base_type)

        return StructType(fields=fields)

    @abstractmethod
    def _parse_type(self, column_props: dict[str, Any]) -> RecapType:
        ...

    def _parse_parameters(self, col_type: str) -> list[str] | None:
        """
        Parse types that have parameters.
        """
        match = re.search(r"\((.*?)\)", col_type)
        if match:
            return [p.strip() for p in match.group(1).split(",")]
        return None

    def _get_time_unit(self, params: list[str] | None) -> str | None:
        match params:
            case [unit, *_] if int(unit) == 0:
                return "second"
            case [unit, *_] if int(unit) <= 3:
                return "millisecond"
            case [unit, *_] if int(unit) <= 6:
                return "microsecond"
