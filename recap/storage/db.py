from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Index, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import func, text
from sqlalchemy.types import JSON, BigInteger, Integer, String

from recap.storage.abstract import AbstractStorage, Direction, MetadataSubtype

Base = declarative_base()


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
    url = Column(String(65535), nullable=False)
    metadata_type = Column(String(255), nullable=False)
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
            "url_type_idx",
            url,
            metadata_type,
        ),
    )

    def is_deleted(self) -> bool:
        return self.deleted_at is not None


class LinkEntry(Base):
    __tablename__ = "links"

    # Sequence instead of autoincrement="auto" for DuckDB compatibility
    link_id_seq = Sequence("link_id_seq")
    id = Column(
        # Use Integer with SQLite since it's suggested by SQLalchemy
        BigInteger().with_variant(Integer, "sqlite"),
        link_id_seq,
        primary_key=True,
    )
    url_from = Column(String(65535), nullable=False)
    url_to = Column(String(65535), nullable=False)
    relationship_type = Column(String(255), nullable=False)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    deleted_at = Column(DateTime)

    __table_args__ = (
        Index(
            "from_to_type_idx",
            url_from,
            url_to,
            relationship_type,
        ),
    )

    def is_deleted(self) -> bool:
        return self.deleted_at is not None


class DatabaseStorage(AbstractStorage):
    def __init__(self, url: str, **engine_opts):
        self.engine = create_engine(url, **engine_opts)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(self.engine)

    def metadata(
        self,
        url: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        with self.Session() as session, session.begin():
            maybe_entry = session.scalar(
                select(
                    MetadataEntry,
                )
                .where(
                    MetadataEntry.url == url,
                    MetadataEntry.metadata_type == metadata_type.__name__.lower(),
                    MetadataEntry.created_at <= (time or func.now()),
                )
                .order_by(
                    MetadataEntry.id.desc(),
                )
            )
            if metadata := maybe_entry:
                if not metadata.is_deleted():
                    return metadata_type.parse_obj(metadata.metadata_obj)

    def links(
        self,
        url: str,
        relationship: str,
        time: datetime | None = None,
        direction: Direction = Direction.FROM,
    ) -> list[str]:
        with self.Session() as session, session.begin():
            subquery = (
                session.query(
                    LinkEntry,
                    func.rank()
                    .over(
                        order_by=LinkEntry.id.desc(),
                        partition_by=(
                            LinkEntry.url_from,
                            LinkEntry.url_to,
                            LinkEntry.relationship_type,
                        ),
                    )
                    .label("rnk"),
                )
                .filter(
                    LinkEntry.url_from == url
                    if direction == Direction.FROM
                    else LinkEntry.url_to == url,
                    LinkEntry.relationship_type == relationship.lower(),
                    LinkEntry.created_at <= (time or func.now()),
                )
                .subquery()
            )
            query = session.query(subquery).filter(
                subquery.c.rnk == 1,
                subquery.c.deleted_at == None,
            )
            rows = session.execute(query).fetchall()
            return [row[2] if direction == Direction.FROM else row[1] for row in rows]

    def search(
        self,
        query: str,
        metadata_type: type[MetadataSubtype],
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
                            MetadataEntry.url,
                            MetadataEntry.metadata_type,
                        ),
                    )
                    .label("rnk"),
                )
                .filter(
                    MetadataEntry.metadata_type == str(metadata_type.__name__).lower(),
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

            return [metadata_type.parse_obj(row[0]) for row in rows]

    def write(
        self,
        url: str,
        metadata: BaseModel,
    ):
        with self.Session() as session, session.begin():
            session.add(
                MetadataEntry(
                    url=url,
                    metadata_type=str(type(metadata).__name__).lower(),
                    metadata_obj=metadata.dict(),
                )
            )

    def link(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        with self.Session() as session, session.begin():
            session.add(
                LinkEntry(
                    url_from=url,
                    url_to=other_url,
                    relationship_type=relationship.lower(),
                )
            )

    def unlink(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        with self.Session() as session, session.begin():
            maybe_entry = session.scalar(
                select(
                    LinkEntry,
                )
                .where(
                    LinkEntry.url_from == url,
                    LinkEntry.url_to == other_url,
                    LinkEntry.relationship_type == relationship.lower(),
                )
                .order_by(
                    LinkEntry.id.desc(),
                )
            )
            if link := maybe_entry:
                link.deleted_at = datetime.now()
                session.commit()
