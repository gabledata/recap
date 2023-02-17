# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-module-docstring

import pytest
from pydantic import BaseModel

from recap.catalog import Relationship
from recap.storage import create_storage
from recap.storage.abstract import AbstractStorage


class TestMetadata(BaseModel):
    field1: str
    field2: int | None


class TestDatabaseStorage:
    @pytest.fixture
    def storage(self) -> AbstractStorage:
        return create_storage("sqlite:///:memory:")

    @pytest.fixture
    def metadata(self):
        return TestMetadata(field1="test", field2=123)

    def test_link_doesnt_exist(self, storage: AbstractStorage):
        url = "postgresql://localhost/some_db/some_schema"
        assert storage.links(url, Relationship.CONTAINS.name) == []

    def test_link_does_exist(self, storage: AbstractStorage):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_table"
        assert storage.links(parent, Relationship.CONTAINS.name) == []

        storage.link(parent, Relationship.CONTAINS.name, child)
        assert storage.links(parent, Relationship.CONTAINS.name) == [child]

    def test_link_readded(self, storage: AbstractStorage):
        parent = "postgresql://localhost/some_db/some_schema"
        child = f"{parent}/some_table"
        assert storage.links(parent, Relationship.CONTAINS.name) == []

        storage.link(parent, Relationship.CONTAINS.name, child)
        assert storage.links(parent, Relationship.CONTAINS.name) == [child]

        storage.unlink(parent, Relationship.CONTAINS.name, child)
        assert storage.links(parent, Relationship.CONTAINS.name) == []

        storage.link(parent, Relationship.CONTAINS.name, child)
        assert storage.links(parent, Relationship.CONTAINS.name) == [child]

    def test_write(self, storage: AbstractStorage, metadata: TestMetadata):
        url = "bigquery://some-project-1234/some_dataset/some_table"
        assert storage.metadata(url, type(metadata)) is None

        storage.write(url, metadata)
        assert storage.metadata(url, type(metadata)) == metadata

    def test_ls_multiple_entries(self, storage: AbstractStorage):
        parent = "bigquery://some-project-1234/some_dataset"
        child_path_one = f"{parent}/some_table_1"
        child_path_two = f"{parent}/some_table_2"
        child_path_three = f"{parent}/some_table_3"
        storage.link(parent, Relationship.CONTAINS.name, child_path_one)
        storage.link(parent, Relationship.CONTAINS.name, child_path_two)
        storage.link(parent, Relationship.CONTAINS.name, child_path_three)
        assert sorted(storage.links(parent, Relationship.CONTAINS.name)) == sorted(
            [child_path_one, child_path_two, child_path_three]
        )

    def test_search(self, storage: AbstractStorage, metadata: TestMetadata):
        url = "bigquery://some-project-1234/some_dataset/some_table"

        storage.write(url, metadata)
        search_result = storage.search(
            "json_extract(metadata_obj, '$.\"field1\"') = 'test'",
            type(metadata),
        )

        assert search_result == [metadata]
