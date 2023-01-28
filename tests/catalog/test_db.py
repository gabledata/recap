import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
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

    def test_catalog_init(self, catalog):
        assert isinstance(catalog.Session(), Session)

    def test_catalog_touch(self, catalog):
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

    def test_write(self, catalog):
        metadata = {
            "db.location": {
                "database": "postgresql",
                "instance": "localhost",
                "schema": "some_db",
                "table": "some_table"
            }
        }
        path = "/foo/bar/baz"

        catalog.write(path, metadata, patch=False)
        assert catalog.read(path) == metadata

    def test_rm(self, catalog):
        catalog.touch("/databases/table")
        catalog.rm("/databases/table")

        with catalog.Session() as session:
            entry = session.query(CatalogEntry).filter_by(parent="/databases", name="table").first()
            assert entry.is_deleted

        assert catalog.ls("/databases/table") == None

    def test_ls_no_entry(self, catalog):
        assert catalog.ls('/databases/schema') == None

    def test_ls_one_entry(self, catalog):
        catalog.write('/databases/schema/table_one', {})
        assert set(catalog.ls('/databases/schema')) == set(['table_one'])

    def test_ls_multiple_entries(self, catalog):
        catalog.write('/databases/schema/table_one', {})
        catalog.write('/databases/schema/table_two', {})
        catalog.write('/databases/schema/table_three', {})
        assert set(catalog.ls('/databases/schema')) == set(['table_one','table_two','table_three'])

    def test_search(self, catalog):
        metadata = {
            "db.location": {
                "database": "postgresql",
                "instance": "localhost",
                "schema": "some_db",
                "table": "some_table"
            }
        }
        path = "/foo/bar/baz"

        catalog.write(path, metadata, patch=False)
        search_result = catalog.search("json_extract(metadata, '$.\"db.location\".table') = 'some_table'")
        assert search_result == [metadata]


