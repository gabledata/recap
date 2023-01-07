from .abstract import AbstractCatalog
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from recap.config import RECAP_HOME, settings
from sqlalchemy import Column, DateTime, delete, create_engine, Index, \
    select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import func, text
from sqlalchemy.types import BigInteger, Integer, String, JSON
from typing import Any, Generator, List
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
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.current_timestamp(),
    )

    __table_args__ = (
        Index(
            'parent_name_idx',
            parent,
            name,
            unique=True,
        ),
    )


class DatabaseCatalog(AbstractCatalog):
    """
    DatabaseCatalog stores metadata entries in a `catalog` table using
    SQLAlchemy. The table has three main columns: parent, name, and metadata.
    The parent and name columns reflect the directory for the metadata (as
    defined by an AbstractBrowser). The metadata column contains a JSON blob of
    all the various metadata types and objects.

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
                with self.Session() as session, session.begin():
                    # TODO An UPSERT would be great here. SQLAlchemy doesn't
                    # seem to support generic UPSERT, though.
                    results = session.execute(select(CatalogEntry).where(
                        CatalogEntry.parent == str(cwd.parent),
                        CatalogEntry.name == str(cwd.name),
                    ))

                    if not results.fetchone():
                        doc = self._get_metadata(session, cwd) or {}
                        session.add(CatalogEntry(
                            parent=str(cwd.parent),
                            name=cwd.name,
                            metadata_=doc,
                        ))
                        session.commit()

    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        path = PurePosixPath('/', path)

        # TODO An UPSERT would be great here. SQLAlchemy doesn't seem to
        # support generic UPSERT, though.
        self.touch(path)
        with self.Session() as session, session.begin():
            doc = self._get_metadata(session, path)
            updated_doc = (doc or {}) | {type: metadata}
            session.execute(update(CatalogEntry).where(
                CatalogEntry.parent == str(path.parent),
                CatalogEntry.name == path.name,
            ).values(metadata_ = updated_doc))

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        path = PurePosixPath('/', path)
        if not type:
            with self.Session() as session:
                session.execute(delete(CatalogEntry).where(
                    (
                        CatalogEntry.parent.match(f"{path}%")
                    ) | (
                        CatalogEntry.parent == str(path.parent),
                        CatalogEntry.name == path.name,
                    )
                ))
        else:
            with self.Session() as session, session.begin():
                doc = self._get_metadata(session, path)
                if doc:
                    doc.pop(type, None)
                    session.execute(update(CatalogEntry).where(
                        CatalogEntry.parent == str(path.parent),
                        CatalogEntry.name == path.name,
                    ).values(metadata_ = doc))

    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        path = PurePosixPath('/', path)
        with self.Session() as session:
            rows = session.execute(select(CatalogEntry.name).where(
                CatalogEntry.parent == str(path)
            )).fetchall()
            # Get the name from each row
            names = list(map(lambda r: r[0], rows)) if rows else None
            return names

    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        path = PurePosixPath('/', path)
        with self.Session() as session:
            return self._get_metadata(session, path)

    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        with self.Session() as session:
            rows = session.execute(select(CatalogEntry.metadata_).where(
                # TODO Yikes. Pretty sure this is a SQL injection vulnerability.
                text(query)
            )).fetchall() or []

            return [row[0] for row in rows]

    def _get_metadata(
        self,
        session: Session,
        path: PurePosixPath,
    ) -> Any | None:
        maybe_entry = session.scalar(select(CatalogEntry).where(
            CatalogEntry.parent == str(path.parent),
            CatalogEntry.name == path.name,
        ))
        return maybe_entry.metadata_ if maybe_entry else None

    @staticmethod
    @contextmanager
    def open(**config) -> Generator['DatabaseCatalog', None, None]:
        url = config.get('url')
        if not url:
            # If no URL is set, default to SQLite
            url = DEFAULT_URL
            # Make sure the catalog directory exists
            db_path = urlparse(url).path # pyright: ignore [reportGeneralTypeIssues]
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        engine_options = config.get('engine', {})
        engine = create_engine(url, **engine_options)
        yield DatabaseCatalog(engine) # pyright: ignore [reportGeneralTypeIssues]
