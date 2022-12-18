import duckdb
import fsspec
import json
from .abstract import AbstractCatalog
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.config import RECAP_HOME, settings
from typing import List, Any, Generator
from urllib.parse import urlparse


DEFAULT_URL = f"file://{settings('root_path', RECAP_HOME)}/catalog/recap.duckdb"


class DuckDbCatalog(AbstractCatalog):
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
    ):
        self.connection = connection
        # TODO should try this and catch if read_only=True
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS catalog "
            "(parent VARCHAR, name VARCHAR, metadata JSON)"
        )
        self.connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS parent_name_idx ON catalog (parent, name)"
        )

    def touch(
        self,
        path: PurePosixPath,
    ):
        path = PurePosixPath('/', path)
        # Reverse to start with root.
        path_stack = list(path.parts)[::-1]
        cwd = '/'

        # Touch all parents to make sure they exist as well
        while len(path_stack):
            cwd = PurePosixPath(cwd, path_stack.pop())

            # PurePosixPath('/').parts returns ('/',). We don't want to touch
            # the root because it doesn't fit the parent/name model that we
            # have.
            if len(cwd.parts) > 1:
                self.connection.begin()

                # TODO An UPSERT would be great here. Doesn't seem DuckDB supports it.
                results = self.connection.execute(
                    "SELECT 1 FROM catalog WHERE parent = ? AND name = ?",
                    [str(cwd.parent), cwd.name]
                )

                if not results.fetchone():
                    doc = self._get_metadata(cwd) or {}
                    self.connection.execute(
                        "INSERT INTO catalog VALUES (?, ?, ?)",
                        [str(cwd.parent), cwd.name, json.dumps(doc)]
                    )

                self.connection.commit()


    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        path = PurePosixPath('/', path)

        # TODO An UPSERT would be great here. Doesn't seem DuckDB supports it.
        self.touch(path)
        self.connection.begin()
        doc = self._get_metadata(path)
        updated_doc = (doc or {}) | {type: metadata}
        self.connection.execute(
            (
                "UPDATE catalog SET metadata = ? "
                "WHERE parent = ? AND name = ?"
            ),
            [json.dumps(updated_doc), str(path.parent), path.name]
        )
        self.connection.commit()

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        path = PurePosixPath('/', path)
        if not type:
            self.connection.execute(
                (
                    "DELETE FROM catalog WHERE "
                    "parent LIKE ? OR (parent = ? AND name = ?)"
                ),
                [f"{path}%", str(path.parent), path.name]
            )
        else:
            self.connection.begin()
            doc = self._get_metadata(path)
            if doc:
                doc.pop(type, None)
                self.connection.execute(
                    (
                        "UPDATE catalog SET metadata = ? "
                        "WHERE parent = ? AND name = ?"
                    ),
                    [json.dumps(doc), str(path.parent), path.name]
                )
            self.connection.commit()

    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        path = PurePosixPath('/', path)
        self.connection.execute(
            "SELECT name FROM catalog WHERE parent = ?", [str(path)]
        )
        rows = self.connection.fetchall()
        # Get the name from each row
        names = list(map(lambda r: r[0], rows)) if rows else None
        return names # pyright: ignore [reportGeneralTypeIssues]

    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        path = PurePosixPath('/', path)
        return self._get_metadata(path)

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []

        self.connection.execute(
            # ZOMG SQL injection. Should figure out a better way to do this.
            f"SELECT metadata FROM catalog WHERE {query}"
        )

        for row in self.connection.fetchall():
            results.append(json.loads(row[0]))

        return results

    def _get_metadata(self, path: PurePosixPath) -> Any | None:
        self.connection.execute(
            (
                "SELECT metadata FROM catalog "
                "WHERE parent = ? AND name = ?"
            ),
            [str(path.parent), path.name]
        )
        maybe_row = self.connection.fetchone()
        return json.loads(maybe_row[0]) if maybe_row else None # pyright: ignore [reportGeneralTypeIssues]


@contextmanager
def open(**config) -> Generator[DuckDbCatalog, None, None]:
    url = urlparse(config.get('url', DEFAULT_URL))
    duckdb_options = config.get('duckdb', {})
    duckdb_dir = PurePosixPath(url.path).parent
    try:
        fsspec \
            .filesystem('file', auto_mkdir=True) \
            .mkdirs(duckdb_dir, exist_ok=True)
    except FileExistsError:
        pass
    with duckdb.connect(url.path, **duckdb_options) as c:
        yield DuckDbCatalog(c)
