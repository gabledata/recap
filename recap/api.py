import fsspec
from .storage import FilesystemTableStorage
from fastapi import Body, FastAPI
from typing import Any, List


app = FastAPI()
# TODO make FS configurable
fs = fsspec.filesystem('file', auto_mkdir=True)
# TODO make root dir configurable
root = '/tmp/recap'


# TODO re-order APIs based on instance, schema, table, view, metadata
@app.put("/databases/{infra}/instances/{instance}")
def put_instance(
    infra: str,
    instance: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.put_instance()


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}")
def put_schema(
    infra: str,
    instance: str,
    schema: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.put_schema(schema)


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}")
def put_table(
    infra: str,
    instance: str,
    schema: str,
    table: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.put_table(schema, table)


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}")
def put_view(
    infra: str,
    instance: str,
    schema: str,
    view: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.put_view(schema, view)


@app.delete("/databases/{infra}/instances/{instance}")
def delete_instance(
    infra: str,
    instance: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.remove_instance()


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}")
def delete_schema(
    infra: str,
    instance: str,
    schema: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.remove_schema(schema)


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}")
def delete_table(
    infra: str,
    instance: str,
    schema: str,
    table: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.remove_table(schema, table)


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}")
def delete_view(
    infra: str,
    instance: str,
    schema: str,
    view: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    metadata_storage.remove_view(schema, view)


@app.get("/databases/{infra}/instances/{instance}/schemas")
def list_schemas(
    infra: str,
    instance: str,
) -> List[str]:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_schemas()


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/tables")
def list_tables(
    infra: str,
    instance: str,
    schema: str,
) -> List[str]:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_tables(schema)


@app.get("/databases/{infra}/{instance}/schemas/{schema}/views")
def list_views(
    infra: str,
    instance: str,
    schema: str,
) -> List[str]:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_views(schema)


@app.get("/databases/{infra}/instances/{instance}/metadata")
def list_instance_metadata(
    infra: str,
    instance: str,
) -> List[str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_metadata()


@app.get("/databases/{infra}/instances/{instance}/metadata/{type}")
def get_instance_metadata(
    infra: str,
    instance: str,
    type: str,
) -> dict[str, str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.get_metadata(type)


@app.put("/databases/{infra}/instances/{instance}/metadata/{type}")
def put_instance_metadata(
    infra: str,
    instance: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.put_metadata(type, metadata)


@app.delete("/databases/{infra}/instances/{instance}/metadata/{type}")
def delete_instance_metadata(
    infra: str,
    instance: str,
    type: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.remove_metadata(type)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata")
def list_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
) -> List[str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_metadata(schema)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata/{type}")
def get_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
    type: str,
) -> dict[str, str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.get_metadata(type, schema)


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata/{type}")
def put_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.put_metadata(type, metadata, schema)


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata/{type}")
def delete_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
    type: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.remove_metadata(type, schema)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata")
def list_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
) -> List[str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_metadata(schema, table=table)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata/{type}")
def get_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    type: str,
) -> dict[str, str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.get_metadata(type, schema, table=table)


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata/{type}")
def put_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.put_metadata(type, metadata, schema, table=table)


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata/{type}")
def delete_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    type: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.remove_metadata(type, schema, table=table)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata")
def list_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
) -> List[str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.list_metadata(schema, view=view)


@app.get("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata/{type}")
def get_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    type: str,
) -> dict[str, str] | None:
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.get_metadata(type, schema, view=view)


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata/{type}")
def put_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.put_metadata(type, metadata, schema, view=view)


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata/{type}")
def delete_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    type: str,
):
    metadata_storage = FilesystemTableStorage(infra, instance, root, fs)
    return metadata_storage.remove_metadata(type, schema, view=view)
