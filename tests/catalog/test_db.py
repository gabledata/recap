import pytest
from sqlalchemy import create_engine
from datetime import datetime
from recap.catalogs.db import DatabaseCatalog, CatalogEntry
from recap.analyzers.sqlalchemy.primary_key import PrimaryKey
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

    def test_catalog_touch_doesnt_exist(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == [str(child_path)]

    def test_catalog_touch_does_exist(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == [str(child_path)]

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == [str(child_path)]

    def test_catalog_touch_deleted_path(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == [str(child_path)]

        catalog.rm(parent_path / child_path)
        assert catalog.ls(parent_path) is None
        assert catalog.read(parent_path / child_path) is None

        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == [str(child_path)]

    def test_write(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        metadata = PrimaryKey(
            name="test",
            constrained_columns=["test"],
        )

        catalog.write(parent_path / child_path, metadata.dict(), patch=False)
        assert catalog.read(parent_path / child_path) == metadata.dict()

    def test_write_metadata_after_rm(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        metadata = PrimaryKey(
            name="test",
            constrained_columns=["test"],
        )

        catalog.write(parent_path / child_path, metadata.dict(), patch=False)
        assert catalog.read(parent_path / child_path) == metadata.dict()

        catalog.rm(parent_path / child_path)
        assert catalog.read(parent_path / child_path) is None

    def test_rm(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")
        catalog.touch(parent_path / child_path)
        assert catalog.ls(parent_path) == sorted([str(child_path)])

        catalog.rm(parent_path / child_path)
        assert catalog.ls(parent_path / child_path) is None

    def test_ls_no_entry(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        assert catalog.ls(parent_path) is None

    def test_ls_one_entry(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("table_one")
        catalog.write(parent_path / child_path, {})
        assert sorted(catalog.ls(parent_path)) == sorted([str(child_path)])

    def test_ls_multiple_entries(self, catalog):
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path_one = Path("table_one")
        child_path_two = Path("table_two")
        child_path_three = Path("table_three")
        catalog.write(parent_path / child_path_one, {})
        catalog.write(parent_path / child_path_two, {})
        catalog.write(parent_path / child_path_three, {})
        assert sorted(catalog.ls(parent_path)) == sorted([str(child_path_one), str(child_path_two), str(child_path_three)])

    def test_search(self, catalog):
        metadata = PrimaryKey(
            name="test",
            constrained_columns=["test"],
        )
        parent_path = Path("databases","postgresql", "instances", "localhost", "schemas", "some_db", "tables")
        child_path = Path("some_table")

        catalog.write(parent_path / child_path, metadata.dict(), patch=False)
        search_result = catalog.search("json_extract(metadata, '$.\"name\"') = 'test'")

        assert search_result == [metadata.dict()]


