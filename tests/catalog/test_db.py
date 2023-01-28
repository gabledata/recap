import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from recap.catalogs.db import DatabaseCatalog, CatalogEntry

class TestCatalogEntry:
    def test_is_deleted(self):
        entry = CatalogEntry(deleted_at=datetime.now())
        assert entry.is_deleted() == True

        entry = CatalogEntry(deleted_at=None)
        assert entry.is_deleted() == False

class TestDatabaseCatalog:
    @pytest.fixture
    def engine(self):
        return create_engine('sqlite:///:memory:')

    @pytest.fixture
    def catalog(self, engine):
        return DatabaseCatalog(engine)

    def test_catalog_init(self, engine, catalog):
        assert isinstance(catalog.Session(), Session)

    def test_catalog_touch(self):
        engine = create_engine("sqlite:///:memory:")
        catalog = DatabaseCatalog(engine)

        # Test touching a path that doesn't exist
        catalog.touch("/databases/table")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/databases", name="table").first()
            assert result.metadata_ == {}

        # Test touching a path that does exist
        catalog.touch("/databases/table")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/databases", name="table").first()
            assert result.metadata_ == {}

        # Test touching a path that has been deleted
        with catalog.Session() as session:
            entry = session.query(CatalogEntry).filter_by(parent="/databases", name="table").first()
            entry.deleted_at = datetime.now()

        catalog.touch("/databases/table")
        with catalog.Session() as session:
            result = session.query(CatalogEntry).filter_by(parent="/databases", name="table").first()
            assert result.metadata_ == {}

    def test_ls(self, catalog):
        catalog.write('/databases/schema/table_one', {})
        catalog.write('/databases/schema/table_two', {})
        catalog.write('/databases/schema/table_three', {})
        assert set(catalog.ls('/databases/schema')) == set(['table_one','table_two','table_three'])

