# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-module-docstring

from datetime import datetime

import pytest
from sqlalchemy import create_engine

from recap.catalogs.db import CatalogEntry, DatabaseCatalog


class TestCatalogEntry:
    def test_is_deleted(self):
        entry = CatalogEntry(deleted_at=datetime.now())
        assert entry.is_deleted() is True

    def test_not_deleted(self):
        entry = CatalogEntry(deleted_at=None)
        assert entry.is_deleted() is False


class TestDatabaseCatalog:
    @pytest.fixture
    def engine(self):
        return create_engine("sqlite:///:memory:")

    @pytest.fixture
    def catalog(self, engine):
        return DatabaseCatalog(engine)

    def test_catalog_touch_doesnt_exist(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_child"

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_child"]

    def test_catalog_touch_does_exist(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_child"

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_child"]

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_child"]

    def test_catalog_touch_deleted_path(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_child"

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_child"]

        catalog.rm(child)
        assert catalog.ls(parent) is None
        assert catalog.read(child) is None

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_child"]

    def test_write(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"

        metadata = {
            "test1": 1,
            "test2": {
                "foo": "bar",
            },
        }

        catalog.write(child, metadata, patch=False)
        assert catalog.read(child) == metadata

    def test_write_metadata_after_rm(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"
        metadata = {
            "test1": 1,
            "test2": {
                "foo": "bar",
            },
        }

        catalog.write(child, metadata, patch=False)
        assert catalog.read(child) == metadata

        catalog.rm(child)
        assert catalog.read(child) is None

    def test_rm(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"

        catalog.touch(child)
        assert catalog.ls(parent) == ["some_table"]

        catalog.rm(child)
        assert catalog.ls(child) is None

    def test_ls_no_entry(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        assert catalog.ls(parent) is None

    def test_ls_one_entry(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"

        catalog.write(child, {})
        assert sorted(catalog.ls(parent)) == ["some_table"]

    def test_ls_multiple_entries(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child_path_one = f"{parent}/some_table_1"
        child_path_two = f"{parent}/some_table_2"
        child_path_three = f"{parent}/some_table_3"
        catalog.write(child_path_one, {})
        catalog.write(child_path_three, {})
        catalog.write(child_path_two, {})
        assert sorted(catalog.ls(parent)) == sorted(
            ["some_table_1", "some_table_2", "some_table_3"]
        )

    def test_search(self, catalog):
        path = "bigquery://some-project-1234/some_dataset/some_table"
        metadata = {
            "test1": 1,
            "test2": {
                "foo": "bar",
            },
        }

        catalog.write(path, metadata, patch=False)
        search_result = catalog.search("json_extract(metadata, '$.\"test1\"') = 1")

        assert search_result == [metadata]
