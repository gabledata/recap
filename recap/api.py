from .config import settings
from . import storage
from .storage.abstract import AbstractStorage
from fastapi import Body, Depends, FastAPI, Request
from typing import Any, List, Generator


app = FastAPI()


def get_storage() -> Generator[AbstractStorage, None, None]:
    with storage.open(**settings['storage']) as s:
        yield s


# TODO re-order APIs based on instance, schema, table, view, metadata
@app.put("/databases/{infra}/instances/{instance}")
def put_instance(
    infra: str,
    instance: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.put_instance(
        infra,
        instance
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}")
def put_schema(
    infra: str,
    instance: str,
    schema: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.put_schema(
        infra,
        instance,
        schema
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}")
def put_table(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.put_table(
        infra,
        instance,
        schema,
        table
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}")
def put_view(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.put_view(
        infra,
        instance,
        schema,
        view
    )


@app.delete("/databases/{infra}/instances/{instance}")
def delete_instance(
    infra: str,
    instance: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.remove_instance(
        infra,
        instance
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}")
def delete_schema(
    infra: str,
    instance: str,
    schema: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.remove_schema(
        infra,
        instance,
        schema
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}")
def delete_table(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.remove_table(
        infra,
        instance,
        schema,
        table
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}")
def delete_view(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.remove_view(
        infra,
        instance,
        schema,
        view
    )


# WARN: This has to go before list() since it's a more specific path
@app.get("/{path:path}/metadata/{type}")
def get_metadata(
    path: str,
    type: str,
    storage: AbstractStorage = Depends(get_storage),
) -> dict[str, str] | None:
    return storage.get_metadata(
        path,
        type,
    )


@app.get("/{path:path}")
def list(
    path: str,
    storage: AbstractStorage = Depends(get_storage),
) -> List[str]:
    return storage.list(path) or []


@app.delete("/databases/{infra}/instances/{instance}/metadata/{type}")
def delete_instance_metadata(
    infra: str,
    instance: str,
    type: str,
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.remove_metadata(
        infra,
        instance,
        type
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata/{type}")
def put_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.put_metadata(
        infra,
        instance,
        type,
        metadata,
        schema
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/metadata/{type}")
def delete_schema_metadata(
    infra: str,
    instance: str,
    schema: str,
    type: str,
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.remove_metadata(
        infra,
        instance,
        type,
        schema
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata/{type}")
def put_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.put_metadata(
        infra,
        instance,
        type,
        metadata,
        schema,
        table=table
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/tables/{table}/metadata/{type}")
def delete_table_metadata(
    infra: str,
    instance: str,
    schema: str,
    table: str,
    type: str,
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.remove_metadata(
        infra,
        instance,
        type,
        schema,
        table=table
    )


@app.put("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata/{type}")
def put_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    type: str,
    metadata: dict[str, Any] = Body(...),
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.put_metadata(
        infra,
        instance,
        type,
        metadata,
        schema,
        view=view
    )


@app.delete("/databases/{infra}/instances/{instance}/schemas/{schema}/views/{view}/metadata/{type}")
def delete_view_metadata(
    infra: str,
    instance: str,
    schema: str,
    view: str,
    type: str,
    storage: AbstractStorage = Depends(get_storage),
):
    return storage.remove_metadata(
        infra,
        instance,
        type,
        schema,
        view=view
    )
