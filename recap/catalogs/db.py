from .abstract import AbstractCatalog
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from recap.config import RECAP_HOME, settings
from sqlalchemy import Column, DateTime, create_engine, Index, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import func, text
from sqlalchemy.types import BigInteger, Integer, String, JSON
from typing import Any, Generator
from urllib.parse import urlparse


DEFAULT_URL = f"sqlite:///{settings('root_path', RECAP_HOME)}/catalog/recap.db"
Base = declarative_base()


class CatalogEntry(Base):
    __tablename__ = 'catalog'

    # Sequence instead of autoincrement="auto" for DuckDB compatibility
    entry_id_seq = Sequence('entry_id_seq')
    id = Column(
        # Use Integer with SQLite since it's suggested by SQLalchemy
        BigInteger().with_variant(Integer, "sqlite"),
        entry_id_seq,
        primary_key=True,
    )
    parent = Column(String(65535), nullable=False)
    name = Column(String(4096), nullable=False)
    metadata_ = Column(
        'metadata',
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
            'parent_name_idx',
            parent,
            name,
        ),
    )

    def is_deleted(self) -> bool:
        return self.deleted_at is not None


class DatabaseCatalog(AbstractCatalog):
    """
    DatabaseCatalog stores metadata entries in a `catalog` table using
    SQLAlchemy. The table has three main columns: parent, name, and metadata.
    The parent and name columns reflect the directory for the metadata (as
    defined by an AbstractBrowser). The metadata column contains a JSON blob of
    all the various metadata types and objects.

    Previous metadata versions are kept in `catalog` as well. A deleted_at
    field is used to tombstone deleted directories. Directories that were
    updated, not deleted, will not have a deleted_at set; there will just be a
    more recent row (as sorted by `id`).

    Reads return the most recent metadata that was written to the path. If the
    most recent record has a deleted_at tombstone, an None is returned.

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

    def _clean_path(self, path: str) -> tuple[str, PurePosixPath]:
        path_posix = PurePosixPath('/', path)
        path_str = str(path_posix)
        return (path_str, path_posix)

    def touch(
        self,
        path: str,
    ):
        _, path_posix = self._clean_path(path)
        path_stack = list(path_posix.parts)
        cwd = '/'

        with self.Session() as session, session.begin():
            # Touch all parents to make sure they exist.
            while len(path_stack):
                cwd = PurePosixPath(cwd, *path_stack)

                # PurePosixPath('/').parts returns ('/',). We don't want to touch
                # the root because it doesn't fit the parent/name model that we
                # have.
                if len(cwd.parts) > 1:
                    maybe_row = session.scalar(
                        select(
                            CatalogEntry,
                        )
                        .filter(
                            CatalogEntry.parent == str(cwd.parent),
                            CatalogEntry.name == str(cwd.name),
                        ).order_by(
                            CatalogEntry.id.desc(),
                        )
                    )

                    if not maybe_row or maybe_row.is_deleted():
                        session.add(CatalogEntry(
                            parent=str(cwd.parent),
                            name=cwd.name,
                            metadata_={},
                        ))
                    else:
                        # Path exists and isn't deleted. We can assume all
                        # parents also exist, so no need to check.
                        break

                path_stack.pop()

    def write(
        self,
        path: str,
        metadata: dict[str, Any],
        patch: bool = True,
    ):
        path_str, path_posix = self._clean_path(path)
        self.touch(path_str)
        with self.Session() as session, session.begin():
            if patch:
                existing_doc = self._get_metadata(session, path_posix) or {}
                metadata = existing_doc | metadata
            session.add(CatalogEntry(
                parent=str(path_posix.parent),
                name=path_posix.name,
                metadata_=metadata,
            ))

    def rm(
        self,
        path: str,
    ):
        _, path_posix = self._clean_path(path)
        with self.Session() as session:
            session.execute(update(CatalogEntry).filter(
                # parent = /foo/bar/baz
                (CatalogEntry.parent == f"{path_posix}")
                # or parent = /foo/bar/baz/%
                | (CatalogEntry.parent.like(f"{path_posix}/%"))
                # or parent = /foo/bar and name = baz
                | (
                    (CatalogEntry.parent == str(path_posix.parent))
                    & (CatalogEntry.name == path_posix.name)
                )
            ).values(
                deleted_at = func.now()
            ).execution_options(synchronize_session=False))

    def ls(
        self,
        path: str,
        as_of: datetime | None = None,
    ) -> list[str] | None:
        path_str, _ = self._clean_path(path)
        with self.Session() as session:
            subquery = session.query(
                CatalogEntry.name,
                CatalogEntry.deleted_at,
                func.rank().over(
                    order_by=CatalogEntry.id.desc(),
                    partition_by=(
                        CatalogEntry.parent,
                        CatalogEntry.name,
                    )
                ).label('rnk')
            ).filter(
                CatalogEntry.parent == path_str,
                CatalogEntry.created_at <= (as_of or func.now()),
            ).subquery()
            query = session.query(subquery).filter(
                subquery.c.rnk == 1,
                subquery.c.deleted_at == None,
            )
            rows = session.execute(query).fetchall()
            return [row[0] for row in rows] or None

    def read(
        self,
        path: str,
        as_of: datetime | None = None,
    ) -> dict[str, Any] | None:
        _, path_posix = self._clean_path(path)
        with self.Session() as session:
            return self._get_metadata(session, path_posix, as_of)

    def search(
        self,
        query: str,
        as_of: datetime | None = None,
    ) -> list[dict[str, Any]]:
        with self.Session() as session:
            subquery = session.query(
                CatalogEntry.metadata_,
                CatalogEntry.deleted_at,
                func.rank().over(
                    order_by=CatalogEntry.id.desc(),
                    partition_by=(
                        CatalogEntry.parent,
                        CatalogEntry.name,
                    )
                ).label('rnk')
            ).filter(
                CatalogEntry.created_at <= (as_of or func.now()),
                # TODO Yikes. Pretty sure this is a SQL injection vulnerability.
                text(query)
            ).subquery()

            query = session.query(subquery).filter(
                subquery.c.rnk == 1,
                subquery.c.deleted_at == None,
            ) # pyright: ignore [reportGeneralTypeIssues]

            rows = session.execute(query).fetchall()

            return [row[0] for row in rows]

    def _get_metadata(
        self,
        session: Session,
        path_posix: PurePosixPath,
        as_of: datetime | None = None,
    ) -> Any | None:
        maybe_entry = session.scalar(
            select(
                CatalogEntry,
            ).where(
                CatalogEntry.parent == str(path_posix.parent),
                CatalogEntry.name == path_posix.name,
                CatalogEntry.created_at <= (as_of or func.now()),
            ).order_by(
                CatalogEntry.id.desc(),
            )
        )
        if maybe_entry and not maybe_entry.is_deleted():
            return maybe_entry.metadata_
        else:
            return None


@contextmanager
def create_catalog(
    url: str | None = None,
    engine: dict[str, Any] = {},
    **_,
) -> Generator['DatabaseCatalog', None, None]:
    if not url:
        # If no URL is set, default to SQLite
        url = DEFAULT_URL
        # Make sure the catalog directory exists
        db_path = urlparse(url).path # pyright: ignore [reportGeneralTypeIssues]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    yield DatabaseCatalog(create_engine(url, **engine))
