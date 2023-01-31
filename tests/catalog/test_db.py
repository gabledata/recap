import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datetime import datetime
from recap.catalogs.db import DatabaseCatalog, CatalogEntry
from pathlib import Path

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

    def test_catalog_touch_doesnt_exist(self, catalog):
        parent_path = Path("/databases/postgresql/instances/")
        child_path = Path("localhost")

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == ["localhost"]

    def test_catalog_touch_does_exist(self, catalog):
        parent_path = Path("/databases/postgresql/instances/")
        child_path = Path("localhost")

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == ["localhost"]

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == ["localhost"]

    @pytest.mark.skip(reason="Waiting to rebase main")
    def test_catalog_touch_deleted_path(self, catalog):
        parent_path = Path("/databases/postgresql/instances/")
        child_path = Path("localhost")
        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == ["localhost"]

        catalog.rm(parent_path / child_path)
        assert catalog.ls(parent_path) is None

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == ["localhost"]

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
        assert catalog.ls("/databases/table") is None

    def test_ls_no_entry(self, catalog):
        assert catalog.ls('/databases/schema') is None

    def test_ls_one_entry(self, catalog):
        catalog.write('/databases/schema/table_one', {})
        assert sorted(catalog.ls('/databases/schema')) == sorted(['table_one'])

    def test_ls_multiple_entries(self, catalog):
        catalog.write('/databases/schema/table_one', {})
        catalog.write('/databases/schema/table_two', {})
        catalog.write('/databases/schema/table_three', {})
        assert sorted(catalog.ls('/databases/schema')) == sorted(['table_one','table_two','table_three'])

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


