# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-module-docstring

from dataclasses import dataclass
from datetime import datetime

import pytest
from sqlalchemy import create_engine

from recap.catalogs.db import DatabaseCatalog, MetadataEntry, PathEntry
from recap.metadata import Metadata


@dataclass
class TestMetadata(Metadata):
    field1: str
    field2: int | None

    @classmethod
    def key(cls):
        return "test"


class TestMetadataEntry:
    def test_is_deleted(self):
        entry = MetadataEntry(deleted_at=datetime.now())
        assert entry.is_deleted() is True

    def test_not_deleted(self):
        entry = MetadataEntry(deleted_at=None)
        assert entry.is_deleted() is False


class TestPathEntry:
    def test_is_deleted(self):
        entry = PathEntry(deleted_at=datetime.now())
        assert entry.is_deleted() is True

    def test_not_deleted(self):
        entry = PathEntry(deleted_at=None)
        assert entry.is_deleted() is False


class TestDatabaseCatalog:
    @pytest.fixture
    def engine(self):
        return create_engine("sqlite:///:memory:")

    @pytest.fixture
    def catalog(self, engine):
        return DatabaseCatalog(engine)

    @pytest.fixture
    def metadata(self):
        return TestMetadata(field1="test", field2=123)

    def test_catalog_children_doesnt_exist(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_table"

        catalog.add(child)
        assert catalog.children(parent) == ["some_table"]

    def test_catalog_children_does_exist(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_table"

        catalog.add(child)
        assert catalog.children(parent) == ["some_table"]

        catalog.add(child)
        assert catalog.children(parent) == ["some_table"]

    def test_catalog_add_deleted_path(self, catalog):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_table"

        catalog.add(child)
        assert catalog.children(parent) == ["some_table"]

        catalog.remove(child)
        assert catalog.children(parent) is None

        catalog.add(child)
        assert catalog.children(parent) == ["some_table"]

    def test_write(self, catalog, metadata):
        url = "bigquery://some-project-1234/some_dataset/some_table"

        assert catalog.read(url, type(metadata)) is None
        catalog.add(url, metadata)
        assert catalog.read(url, type(metadata)) == metadata

    def test_write_metadata_after_rm(self, catalog, metadata):
        url = "bigquery://some-project-1234/some_dataset/some_table"

        assert catalog.read(url, type(metadata)) is None

        catalog.add(url, metadata)
        assert catalog.read(url, type(metadata)) == metadata

        catalog.remove(url, type(metadata))
        assert catalog.read(url, type(metadata)) is None

    def test_rm_path(self, catalog, metadata):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"

        assert catalog.read(child, type(metadata)) is None
        assert catalog.children(parent) is None

        catalog.add(child, metadata)
        assert catalog.read(child, type(metadata)) == metadata
        assert catalog.children(parent) == ["some_table"]

        catalog.remove(parent)
        assert catalog.read(child, type(metadata)) is None
        assert catalog.children(parent) is None

    def test_ls_no_entry(self, catalog):
        url = "bigquery://some-project-1234/some_dataset"
        assert catalog.children(url) is None

    def test_ls_one_entry(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child = f"{parent}/some_table"

        catalog.add(child)
        assert sorted(catalog.children(parent)) == ["some_table"]

    def test_ls_multiple_entries(self, catalog):
        parent = "bigquery://some-project-1234/some_dataset"
        child_path_one = f"{parent}/some_table_1"
        child_path_two = f"{parent}/some_table_2"
        child_path_three = f"{parent}/some_table_3"
        catalog.add(child_path_one)
        catalog.add(child_path_three)
        catalog.add(child_path_two)
        assert sorted(catalog.children(parent)) == sorted(
            ["some_table_1", "some_table_2", "some_table_3"]
        )

    def test_search(self, catalog, metadata):
        path = "bigquery://some-project-1234/some_dataset/some_table"

        catalog.add(path, metadata)
        search_result = catalog.search(
            "json_extract(metadata_obj, '$.\"field1\"') = 'test'",
            type(metadata),
        )

        assert search_result == [metadata]
