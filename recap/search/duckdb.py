import duckdb
import json
from .abstract import AbstractSearch, AbstractIndexer
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import List, Any, Generator
from urllib.parse import urlparse


class DuckDbSearch(AbstractSearch):
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
    ):
        self.connection = connection
        # TODO should try this and catch if read_only=True
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS catalog "
            "(path VARCHAR, metadata JSON)"
        )

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []

        self.connection.execute(
            # ZOMG SQL injection. Should figure out a better way to do this.
            f"SELECT metadata FROM catalog WHERE {query}"
        )

        for row in self.connection.fetchall():
            results.append(json.loads(row[0]))

        return results


class DuckDbIndexer(AbstractIndexer):
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
    ):
        self.connection = connection
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS catalog "
            "(path VARCHAR, metadata JSON)"
        )

    def written(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        # TODO An UPSERT would be great here. Doesn't seem DuckDB supports it.
        self.connection.begin()
        doc = self._get_metadata(path)
        new_doc = len(doc) == 0
        doc[type] = metadata

        if new_doc:
            self.connection.execute(
                "INSERT INTO catalog VALUES (?, ?)",
                [str(path), json.dumps(doc)]
            )
        else:
            self.connection.execute(
                "UPDATE catalog SET metadata = ? WHERE path = ?",
                [json.dumps(doc), str(path)]
            )

        self.connection.commit()

    def removed(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        if not type:
            self.connection.execute(
                "DELETE FROM catalog WHERE path=?",
                [str(path)]
            )
        else:
            self.connection.begin()
            doc = self._get_metadata(path)
            doc.pop(type, None)
            self.connection.execute(
                "UPDATE catalog SET metadata = ? WHERE path = ?",
                [json.dumps(doc), str(path)]
            )
            self.connection.commit()

    def _get_metadata(self, path: PurePosixPath) -> Any:
        self.connection.execute(
            "SELECT metadata FROM catalog WHERE path = ?", [str(path)]
        )
        maybe_row = self.connection.fetchone()
        return json.loads(maybe_row[0]) if maybe_row else {} # pyright: ignore [reportGeneralTypeIssues]

@contextmanager
def open_search(**config) -> Generator[DuckDbSearch, None, None]:
    url = urlparse(config['url'])
    read_only = config.get('read_only', False)
    duckdb_options = config.get('duckdb', {})
    with duckdb.connect(url.path, read_only=read_only, **duckdb_options) as c:
        yield DuckDbSearch(c)


@contextmanager
def open_indexer(**config) -> Generator[DuckDbIndexer, None, None]:
    url = urlparse(config['url'])
    duckdb_options = config.get('duckdb', {})
    with duckdb.connect(url.path, **duckdb_options) as c:
        yield DuckDbIndexer(c)
