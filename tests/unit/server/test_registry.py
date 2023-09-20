import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import AnyUrl

from recap.server.registry import router, settings
from recap.types import IntType, StructType, to_dict

client = TestClient(router)


@pytest.fixture(autouse=True)
def set_env_variable(tmp_path):
    settings.registry_storage_url = AnyUrl(tmp_path.as_uri())


def test_ls_empty():
    response = client.get("/registry/")
    assert response.status_code == 200
    assert response.json() == []


def test_post_and_get_latest():
    name = "test"
    type_ = StructType(fields=[IntType(bits=24)])

    # Post a new type
    response = client.post(
        f"/registry/{name}",
        json=to_dict(type_),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "1"

    # Get the latest type
    response = client.get(f"/registry/{name}")
    assert response.status_code == 200
    data = response.json()
    assert to_dict(type_) == data[0]


def test_post_and_get_specific_version():
    name = "test_version"
    type1 = StructType(fields=[IntType(bits=32)])
    type2 = StructType(fields=[IntType(bits=24)])

    # Post two types
    response = client.post(
        f"/registry/{name}",
        json=to_dict(type1),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "1"
    response = client.post(
        f"/registry/{name}",
        json=to_dict(type2),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "2"

    # Get version 1
    response = client.get(f"/registry/{name}/versions/1")
    assert response.status_code == 200
    type_dict, version = response.json()
    assert to_dict(type1) == type_dict
    assert version == 1

    # Get version 2
    response = client.get(f"/registry/{name}/versions/2")
    assert response.status_code == 200
    type_dict, version = response.json()
    assert to_dict(type2) == type_dict
    assert version == 2


def test_versions_endpoint():
    name = "test_versions_endpoint"
    type1 = StructType(fields=[IntType(bits=32)])
    type2 = StructType(fields=[IntType(bits=24)])
    type3 = StructType(fields=[IntType(bits=16)])

    # Post three types for the same name
    response = client.post(
        f"/registry/{name}",
        json=to_dict(type1),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "1"

    response = client.post(
        f"/registry/{name}",
        json=to_dict(type2),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "2"

    response = client.post(
        f"/registry/{name}",
        json=to_dict(type3),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    assert response.text == "3"

    # Get versions for the name
    response = client.get(f"/registry/{name}/versions")
    assert response.status_code == 200
    versions = response.json()
    assert versions == [1, 2, 3]


def test_ls_non_empty():
    name1 = "test_schema1"
    name2 = "test_schema2"
    type1 = StructType(fields=[IntType(bits=16)])
    type2 = StructType(fields=[IntType(bits=8)])

    # Post two types under different names
    response = client.post(
        f"/registry/{name1}",
        json=to_dict(type1),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200

    response = client.post(
        f"/registry/{name2}",
        json=to_dict(type2),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200

    # Get the list of schemas
    response = client.get("/registry/")
    assert response.status_code == 200
    schema_names = response.json()
    assert schema_names == [name1, name2]


def test_put_existing_schema_conflict():
    name = "test_put_schema_conflict"
    original_type = StructType(fields=[IntType(bits=16)])
    new_type = StructType(fields=[IntType(bits=8)])

    # Post the original type
    response = client.post(
        f"/registry/{name}",
        json=to_dict(original_type),
        headers={"Content-Type": "application/x-recap+json"},
    )
    assert response.status_code == 200
    version = int(response.text)

    # Try to overwrite the original type with new_type for the same version
    with pytest.raises(HTTPException) as exc_info:
        client.put(
            f"/registry/{name}/versions/{version}",
            json=to_dict(new_type),
            headers={"Content-Type": "application/x-recap+json"},
        )

    assert exc_info.value.status_code == 409

    # Fetch the schema with the same name and version to verify it remains unchanged
    response = client.get(f"/registry/{name}/versions/{version}")
    assert response.status_code == 200
    fetched_type_dict, fetched_version = response.json()

    # Confirm the fetched type is still the original_type
    assert fetched_version == version
    assert fetched_type_dict == to_dict(original_type)
