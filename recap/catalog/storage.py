from datetime import datetime

from sqlalchemy import Column, DateTime, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import func
from sqlalchemy.types import JSON, BigInteger, Integer, String

from recap.models import Schema

Base = declarative_base()


class SchemaEntry(Base):
    __tablename__ = "schemas"

    # Sequence instead of autoincrement="auto" for DuckDB compatibility
    metadata_id_seq = Sequence("schema_id_seq")
    id = Column(
        # Use Integer with SQLite since it's suggested by SQLAlchemy
        BigInteger().with_variant(Integer, "sqlite"),
        metadata_id_seq,
        primary_key=True,
    )
    url = Column(
        String(65535),
        nullable=False,
        index=True,
    )
    schema = Column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=True,
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        index=True,
    )


class Storage:
    def __init__(self, url: str, **engine_opts):
        self.engine = create_engine(url, **engine_opts)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(self.engine)

    def get_schema(
        self,
        url: str,
        time: datetime | None = None,
    ) -> Schema | None:
        with self.Session() as session, session.begin():
            maybe_entry = session.scalar(
                select(
                    SchemaEntry,
                )
                .where(
                    SchemaEntry.url == url,
                    SchemaEntry.created_at <= (time or func.now()),
                )
                .order_by(
                    SchemaEntry.id.desc(),
                )
            )
            if metadata := maybe_entry:
                if metadata.schema:
                    return Schema.parse_obj(metadata.schema)

    def put_schema(
        self,
        url: str,
        schema: Schema,
        time: datetime | None = None,
    ):
        with self.Session() as session, session.begin():
            # TODO Should skip put if recap schema hasn't changed.
            session.add(
                SchemaEntry(
                    url=url,
                    schema=schema.dict(
                        exclude_defaults=True,
                        exclude_unset=True,
                    ),
                    created_at=(time or func.now()),
                )
            )

    def delete_schema(self, url: str, time: datetime | None = None):
        with self.Session() as session, session.begin():
            session.add(
                SchemaEntry(
                    url=url,
                    schema=None,
                    created_at=(time or func.now()),
                )
            )


def create_storage(url: str | None = None, **storage_opts) -> Storage:
    from recap.config import settings

    storage_settings = settings.storage_settings
    combined_opts = storage_settings.opts | storage_opts
    url = url or storage_settings.url

    return Storage(url, **combined_opts)
