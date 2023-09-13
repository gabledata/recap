from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

from pymetastore.metastore import HMS, HTable
from pymetastore.stats import (
    BinaryTypeStats,
    BooleanTypeStats,
    DateTypeStats,
    DecimalTypeStats,
    DoubleTypeStats,
    LongTypeStats,
    StringTypeStats,
    TypeStats,
)

from recap.converters.hive_metastore import HiveMetastoreConverter
from recap.types import RecapType, StructType


class HiveMetastoreClient:
    def __init__(self, client: HMS):
        self.client = client

    @staticmethod
    @contextmanager
    def create(host: str, port: int, **_) -> Generator[HiveMetastoreClient, None, None]:
        with HMS.create(host, port) as client:
            yield HiveMetastoreClient(client)

    @staticmethod
    def parse(
        method: str,
        url: str,
        paths: list[str],
        include_stats: str | None = None,
        **url_args,
    ) -> tuple[str, list[Any]]:
        clean_url = f"thrift://{url_args['host']}"
        if url_args["port"]:
            clean_url += f":{url_args['port']}"
        match method:
            case "ls":
                return (clean_url, paths)
            case "schema" if len(paths) >= 2:
                return (clean_url, paths + [include_stats is not None])
            case _:
                raise ValueError("Invalid method")

    def ls(self, database: str | None = None, table: str | None = None) -> list[str]:
        match (database, table):
            case (None, None):
                return self.ls_databases()
            case (str(database), None):
                return self.ls_tables(database)
            case (str(database), str(table)):
                return [table]
            case _:
                raise ValueError("Invalid arguments")

    def ls_databases(self) -> list[str]:
        return self.client.list_databases()

    def ls_tables(self, database: str) -> list[str]:
        return self.client.list_tables(database)

    def schema(
        self,
        database: str,
        table: str,
        include_stats: bool = False,
    ) -> StructType:
        htable = self.client.get_table(database, table)
        struct = HiveMetastoreConverter().to_recap(htable)
        if include_stats:
            self._load_stats(htable, struct.fields)
        return struct

    def _load_stats(self, table: HTable, fields: list[RecapType]) -> None:
        fields_dict = {f.extra_attrs["name"]: f for f in fields}
        table_stats = self.client.get_table_stats(table, [])
        table_stats = [cs for cs in table_stats if cs.stats is not None]
        for col_stats in table_stats:
            recap_type = fields_dict[col_stats.columnName]
            match col_stats.stats:
                case LongTypeStats() | DoubleTypeStats():
                    recap_type.extra_attrs |= {
                        "low": col_stats.stats.lowValue,
                        "high": col_stats.stats.highValue,
                        "cardinality": col_stats.stats.cardinality,
                    }
                case DateTypeStats():
                    stats: dict[str, Any] = {
                        "cardinality": col_stats.stats.cardinality,
                    }
                    if col_stats.stats.lowValue is not None:
                        stats["low"] = col_stats.stats.lowValue.daysSinceEpoch
                    if col_stats.stats.highValue is not None:
                        stats["high"] = col_stats.stats.highValue.daysSinceEpoch
                    recap_type.extra_attrs |= stats
                case StringTypeStats():
                    recap_type.extra_attrs |= {
                        "average_length": col_stats.stats.avgColLen,
                        "max_length": col_stats.stats.maxColLen,
                        "cardinality": col_stats.stats.cardinality,
                    }
                case BooleanTypeStats():
                    recap_type.extra_attrs |= {
                        "true_count": col_stats.stats.numTrues,
                        "false_count": col_stats.stats.numFalses,
                    }
                case BinaryTypeStats():
                    recap_type.extra_attrs |= {
                        "average_length": col_stats.stats.avgColLen,
                        "max_length": col_stats.stats.maxColLen,
                    }
                case DecimalTypeStats():
                    stats: dict[str, Any] = {
                        "cardinality": col_stats.stats.cardinality,
                    }
                    if col_stats.stats.lowValue is not None:
                        stats["low"] = col_stats.stats.lowValue
                    if col_stats.stats.highValue is not None:
                        stats["high"] = col_stats.stats.highValue
                    recap_type.extra_attrs |= stats

            if isinstance(col_stats.stats, TypeStats):
                recap_type.extra_attrs["null_count"] = col_stats.stats.numNulls
