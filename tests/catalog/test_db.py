import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datetime import datetime
from recap.catalogs.db import DatabaseCatalog, CatalogEntry
from recap.analyzers.db.location import Location
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
        parent_path = "/databases/postgresql/instances/localhost/schemas/some_db/tables"
        metadata = Location(database="postgresql",
                            instance="localhost",
                            schema="some_db",
                            table="some_table")
        catalog.write(parent_path, metadata.dict(), patch=False)
        assert catalog.read(parent_path) == metadata.dict()

    def test_rm(self, catalog):
        catalog.touch("/databases/table")
        assert catalog.ls("/databases") == sorted(["table"])

        catalog.rm("/databases/table")
        assert catalog.ls("/databases/table") is None

    def test_ls_no_entry(self, catalog):
        parent_path = "/databases/postgresql/instances/localhost/schemas/some_db/tables"
        assert catalog.ls(parent_path) is None

    def test_ls_one_entry(self, catalog):
        parent_path = "/databases/postgresql/instances/localhost/schemas/some_db/tables"
        catalog.write(f"{parent_path}/table_one", {})
        assert sorted(catalog.ls(parent_path)) == sorted(['table_one'])

    def test_ls_multiple_entries(self, catalog):
        parent_path = "/databases/postgresql/instances/localhost/schemas/some_db/tables"
        catalog.write(f'{parent_path}/table_one', {})
        catalog.write(f'{parent_path}/table_two', {})
        catalog.write(f'{parent_path}/table_three', {})
        assert sorted(catalog.ls(parent_path)) == sorted(['table_one','table_two','table_three'])

    def test_search(self, catalog):
        metadata = Location(database="db",
                            instance="localhost",
                            schema="some_db",
                            table="some_table")

        path = "/databases/postgresql/instances/localhost/schemas/some_db/tables/some_table"

        catalog.write(path, metadata.dict(), patch=False)
        search_result = catalog.search(
            "json_extract(metadata, '$.\"database\"') = 'db'"
            )

        assert search_result == [metadata.dict()]

