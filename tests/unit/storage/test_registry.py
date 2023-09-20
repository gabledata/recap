import pytest

from recap.storage.registry import RegistryStorage
from recap.types import IntType, StructType


@pytest.fixture
def registry_storage(tmp_path):
    storage_url = f"file://{tmp_path}"
    return RegistryStorage(storage_url)


def test_ls_empty(registry_storage):
    assert registry_storage.ls() == []


def test_put_get_single_item(registry_storage):
    name = "test"
    type_ = StructType(fields=[IntType(bits=24)])
    version = registry_storage.put(name, type_)

    retrieved_type, retrieved_version = registry_storage.get(name)

    assert retrieved_version == version
    assert retrieved_type == type_


def test_put_automatic_versioning(registry_storage):
    name = "test"
    type1 = StructType(fields=[IntType(bits=32)])
    type2 = StructType(fields=[IntType(bits=24)])

    version1 = registry_storage.put(name, type1)
    version2 = registry_storage.put(name, type2)

    assert version2 == version1 + 1


def test_versions(registry_storage):
    name = "test_versions"
    type_ = StructType(fields=[IntType(bits=32)])

    versions = [registry_storage.put(name, type_) for _ in range(5)]

    assert registry_storage.versions(name) == versions


def test_latest(registry_storage):
    name = "test_latest"
    type_ = StructType(fields=[IntType(bits=32)])

    versions = [registry_storage.put(name, type_) for _ in range(3)]
    assert registry_storage.latest(name) == max(versions)


def test_get_non_existent(registry_storage):
    assert registry_storage.get("non_existent") is None


def test_versions_non_existent(registry_storage):
    assert registry_storage.versions("non_existent") is None


def test_get_specific_version(registry_storage):
    name = "test_version"
    type1 = StructType(fields=[IntType(bits=48)])
    type2 = StructType(fields=[IntType(bits=24)])

    version1 = registry_storage.put(name, type1)
    version2 = registry_storage.put(name, type2)

    retrieved_type1, retrieved_version1 = registry_storage.get(name, version1)
    retrieved_type2, retrieved_version2 = registry_storage.get(name, version2)

    assert retrieved_version1 == version1
    assert retrieved_version2 == version2
    assert version1 + 1 == version2

    assert retrieved_type1 == type1
    assert retrieved_type2 == type2
