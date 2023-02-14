from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Generator
from urllib.parse import urlparse

from sqlalchemy import Column, DateTime, Index, create_engine, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import func, text
from sqlalchemy.types import JSON, BigInteger, Integer, String

from recap.config import RECAP_HOME, settings
from recap.metadata import Metadata, MetadataSubtype
from recap.url import URL

from .abstract import AbstractCatalog

DEFAULT_URL = f"sqlite:///{settings('root_path', RECAP_HOME)}/catalog/recap.db"
Base = declarative_base()


class PathEntry(Base):
    __tablename__ = "paths"

    # Sequence instead of autoincrement="auto" for DuckDB compatibility
    path_id_seq = Sequence("path_id_seq")
    id = Column(
        # Use Integer with SQLite since it's suggested by SQLalchemy
        BigInteger().with_variant(Integer, "sqlite"),
        path_id_seq,
        primary_key=True,
    )

    # e.g. "/postgresql/localhost/some_db"
    parent = Column(String(65535), nullable=False)

    # e.g. "some_table"
    name = Column(String(255), nullable=False)

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    deleted_at = Column(DateTime)

    __table_args__ = (
        Index(
            "parent_name_idx",
            parent,
            name,
        ),
    )

    def is_deleted(self) -> bool:
        return self.deleted_at is not None


class MetadataEntry(Base):
    __tablename__ = "metadata"

    # Sequence instead of autoincrement="auto" for DuckDB compatibility
    metadata_id_seq = Sequence("metadata_id_seq")
    id = Column(
        # Use Integer with SQLite since it's suggested by SQLalchemy
        BigInteger().with_variant(Integer, "sqlite"),
        metadata_id_seq,
        primary_key=True,
    )

    # e.g. "/postgresql/localhost/some_db/some_table"
    path = Column(String(65535), nullable=False)

    # e.g. "schema", "lineage", "histogram"
    metadata_type = Column(String(255), nullable=False)

    # e.g. None, "b3dccfde-0e9c-4585-a8bd-815b28e83101", "1234"
    # None is used when type:metadata cardinality is 1:1.
    # For examples, tables have only one schema.
    metadata_id = Column(String(255))

    # e.g. '{"fields": [{"name": "some_field", "type": "int32"}]}'
    metadata_obj = Column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )

    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    deleted_at = Column(DateTime)

    __table_args__ = (
        Index(
            "path_type_id_idx",
            path,
            metadata_type,
            metadata_id,
        ),
    )

    def is_deleted(self) -> bool:
        return self.deleted_at is not None


class DatabaseCatalog(AbstractCatalog):
    """
    The database catalog uses [SQLAlchemy](https://www.sqlalchemy.org/) to
    persists catalog data. By default, a SQLite database is used; the file is
    located in `~/.recap/catalog/recap.db`. Search is implemented using
    SQLite's [json_extract syntax](https://www.sqlite.org/json1.html#the_json_extract_function)
    syntax. See [Recap CLI](cli.md) for an example.

    # Usage

    You can configure the SQLite catalog in your `settings.toml` like so:

    ```toml
    [catalog]
    url = "sqlite://"
    engine.connect_args.check_same_thread = false
    ```

    Anything under the `engine` namespace will be forwarded to the SQLAlchemy
    engine.

    You can use any
    [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/) with the
    database catalog. Here's a `settings.toml` that's configured for
    PostgreSQL:

    ```toml
    [catalog]
    url = "postgresql://user:pass@localhost/some_db"
    ```

    # Implementation

    DatabaseCatalog stores metadata entries in a `catalog` table using
    SQLAlchemy. The table has three main columns: parent, name, and metadata.
    The parent and name columns reflect the directory for the metadata (as
    defined by an AbstractBrowser). The metadata column contains a JSON blob of
    all the various metadata types and objects.

    Previous metadata versions are kept in `catalog` as well. A `deleted_at`
    field is used to tombstone deleted directories. Directories that were
    updated, not deleted, will not have a `deleted_at` set; there will just be
    a more recent row (as sorted by `id`).

    Reads return the most recent metadata that was written to the path. If the
    most recent record has a `deleted_at` tombstone, a None is returned.

    Search strings are simply passed along to the WHERE clause in a SELECT
    statement. This does leave room for SQL injection attacks; not thrilled
    about that.
    """

    def __init__(
        self,
        engine: Engine,
    ):
        self.engine = engine
        Base.metadata.create_all(engine)
        self.Session = sessionmaker(engine)

    def _touch(
        self,
        path: PurePosixPath,
    ):
        path_stack = list(path.parts)
        cwd = PurePosixPath(*path_stack)

        with self.Session() as session, session.begin():
            # Touch all parents to make sure they exist.
            while len(path_stack):
                maybe_row = session.scalar(
                    select(
                        PathEntry,
                    )
                    .filter(
                        PathEntry.parent == str(cwd.parent),
                        PathEntry.name == str(cwd.name),
                    )
                    .order_by(
                        PathEntry.id.desc(),
                    )
                )

                if not maybe_row or maybe_row.is_deleted():
                    session.add(
                        PathEntry(
                            parent=str(cwd.parent),
                            name=cwd.name,
                        )
                    )
                else:
                    # Path exists and isn't deleted. We can assume all
                    # parents also exist, so no need to check.
                    break

                path_stack.pop()
                cwd = PurePosixPath(*path_stack)

    def add(
        self,
        url: str,
        metadata: Metadata | None = None,
    ):
        recap_url = URL(url)
        path = recap_url.dialect_host_port_path
        self._touch(path)
        if metadata:
            with self.Session() as session, session.begin():
                session.add(
                    MetadataEntry(
                        path=str(path),
                        metadata_type=metadata.key(),
                        metadata_id=metadata.id(),
                        metadata_obj=metadata.to_dict(),
                    )
                )

    def remove(
        self,
        url: str,
        type: type[Metadata] | None = None,
        id: str | None = None,
    ):
        recap_url = URL(url)
        path = recap_url.dialect_host_port_path
        if type:
            self._remove_metadata(path, type, id)
        else:
            self._remove_path(path)

    def _remove_path(self, path: PurePosixPath):
        with self.Session() as session:
            session.execute(
                update(PathEntry)
                .filter(
                    # Delete all direct descendants: parent = /foo/bar/baz
                    (PathEntry.parent == str(path))
                    # Delete all indirect descendants: parent = /foo/bar/baz/%
                    | (PathEntry.parent.like(f"{path}/%"))
                    # Delete exact match: parent = /foo/bar and name = baz
                    | (
                        (PathEntry.parent == str(path.parent))
                        & (PathEntry.name == path.name)
                    )
                )
                .values(deleted_at=func.now())
                .execution_options(synchronize_session=False)
            )

            session.execute(
                update(MetadataEntry)
                .filter(
                    # Delete path metadata: parent = /foo/bar/baz
                    (MetadataEntry.path == str(path))
                    # Delete all descendant metadata: parent = /foo/bar/baz/%
                    | (MetadataEntry.path.like(f"{path}/%"))
                )
                .values(deleted_at=func.now())
                .execution_options(synchronize_session=False)
            )

            # Have to commit since synchronize_session=False. Have to set
            # synchronize_session=False because BinaryExpression isn't
            # supported in the filter otherwise.
            session.commit()

    def _remove_metadata(
        self, path: PurePosixPath, type: type[Metadata], id: str | None
    ):
        with self.Session() as session:
            session.execute(
                update(MetadataEntry)
                .filter(
                    # Delete path metadata: parent = /foo/bar/baz
                    (MetadataEntry.path == str(path))
                    # Delete all descendant metadata: parent = /foo/bar/baz/%
                    & (MetadataEntry.metadata_type == type.key())
                    & ((MetadataEntry.metadata_id == id) if id is not None else True)
                )
                .values(deleted_at=func.now())
                .execution_options(synchronize_session=False)
            )

            # Have to commit since synchronize_session=False. Have to set
            # synchronize_session=False because BinaryExpression isn't
            # supported in the filter otherwise.
            session.commit()

    def children(
        self,
        url: str,
        time: datetime | None = None,
    ) -> list[str] | None:
        recap_url = URL(url)
        path = recap_url.dialect_host_port_path
        with self.Session() as session:
            subquery = (
                session.query(
                    PathEntry.name,
                    PathEntry.deleted_at,
                    func.rank()
                    .over(
                        order_by=PathEntry.id.desc(),
                        partition_by=(
                            PathEntry.parent,
                            PathEntry.name,
                        ),
                    )
                    .label("rnk"),
                )
                .filter(
                    PathEntry.parent == str(path),
                    PathEntry.created_at <= (time or func.now()),
                )
                .subquery()
            )
            query = session.query(subquery).filter(
                subquery.c.rnk == 1,
                subquery.c.deleted_at == None,
            )
            rows = session.execute(query).fetchall()
            if rows:
                return [row[0] for row in rows]
            else:
                maybe_path = session.scalar(
                    select(
                        PathEntry,
                    )
                    .where(
                        PathEntry.parent == str(path.parent),
                        PathEntry.name == path.name,
                        MetadataEntry.created_at <= (time or func.now()),
                    )
                    .order_by(
                        MetadataEntry.id.desc(),
                    )
                )
                # Return an empty list of path exists or None if not.
                return [] if maybe_path and not maybe_path.is_deleted() else None

    def read(
        self,
        url: str,
        type: type[MetadataSubtype],
        id: str | None = None,
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        recap_url = URL(url)
        path = recap_url.dialect_host_port_path
        with self.Session() as session:
            maybe_entry = session.scalar(
                select(
                    MetadataEntry,
                )
                .where(
                    MetadataEntry.path == str(path),
                    MetadataEntry.metadata_type == type.key(),
                    MetadataEntry.metadata_id == id if id else True,
                    MetadataEntry.created_at <= (time or func.now()),
                )
                .order_by(
                    MetadataEntry.id.desc(),
                )
            )
            if maybe_entry and not maybe_entry.is_deleted():
                return type.from_dict(maybe_entry.metadata_obj)

    def all(
        self,
        url: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype] | None:
        recap_url = URL(url)
        path = recap_url.dialect_host_port_path
        return self.search(f"path = {path}", type, time)

    def search(
        self,
        query: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        with self.Session() as session:
            subquery = (
                session.query(
                    MetadataEntry.metadata_obj,
                    MetadataEntry.metadata_type,
                    MetadataEntry.deleted_at,
                    func.rank()
                    .over(
                        order_by=MetadataEntry.id.desc(),
                        partition_by=(
                            MetadataEntry.path,
                            MetadataEntry.metadata_type,
                        ),
                    )
                    .label("rnk"),
                )
                .filter(
                    MetadataEntry.metadata_type == type.key(),
                    MetadataEntry.created_at <= (time or func.now()),
                    # TODO Yikes. Pretty sure this is a SQL injection vulnerability.
                    text(query),
                )
                .subquery()
            )

            query = session.query(subquery).filter(
                subquery.c.rnk == 1,
                subquery.c.deleted_at == None,
            )  # pyright: ignore [reportGeneralTypeIssues]

            rows = session.execute(query).fetchall()

            return [type.from_dict(row[0]) for row in rows]


@contextmanager
def create_catalog(
    url: str | None = None,
    engine: dict[str, Any] = {},
    **_,
) -> Generator["DatabaseCatalog", None, None]:
    if not url:
        # If no URL is set, default to SQLite
        url = DEFAULT_URL
        # Make sure the catalog directory exists
        db_path = urlparse(url).path  # pyright: ignore [reportGeneralTypeIssues]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    yield DatabaseCatalog(create_engine(url, **engine))
