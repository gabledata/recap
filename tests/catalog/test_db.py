import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from recap.catalogs.db import DatabaseCatalog, CatalogEntry

class TestCatalogEntry:
    def test_catalog_entry_is_deleted(self):
        entry = CatalogEntry(deleted_at=datetime.now())
        assert entry.is_deleted() == True

        entry = CatalogEntry(deleted_at=None)
        assert entry.is_deleted() == False

class TestDatabaseCatalog:
    def test_database_catalog_init(self):
        engine = create_engine("sqlite:///:memory:")
        catalog = DatabaseCatalog(engine)
        assert isinstance(catalog.Session(), Session)

    def test_database_catalog_touch(self):
        engine = create_engine("sqlite:///:memory:")
        catalog = DatabaseCatalog(engine)

        # Test touching a path that doesn't exist
        catalog.touch("/path/to/file")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/path/to", name="file").first()
            assert result.metadata_ == {}

        # Test touching a path that does exist
        catalog.touch("/path/to/file")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/path/to", name="file").first()
            assert result.metadata_ == {}

        # Test touching a path that has been deleted
        with catalog.Session() as session:
            entry = session.query(CatalogEntry).filter_by(parent="/path/to", name="file").first()
            entry.deleted_at = datetime.now()

        catalog.touch("/path/to/file")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/path/to", name="file").first()
            assert result.metadata_ == {}