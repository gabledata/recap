from datetime import datetime
from typing import Generator

from fastapi import APIRouter, Depends, FastAPI, HTTPException

from recap import diff
from recap.catalog.storage import Storage, create_storage
from recap.models import Diff, Schema

schema_router = APIRouter(
    prefix="/schema",
)


def get_storage() -> Generator[Storage, None, None]:
    yield create_storage()


@schema_router.get("/diff")
def diff_schema(
    url_1: str,
    url_2: str,
    url_time_1: datetime | None = None,
    url_time_2: datetime | None = None,
    storage: Storage = Depends(get_storage),
) -> list[Diff]:
    schema_1 = storage.get_schema(url_1, url_time_1)
    schema_2 = storage.get_schema(url_2, url_time_2)
    if schema_1 and schema_2:
        return diff(schema_1, schema_2)
    raise HTTPException(status_code=404)


@schema_router.get("/{url:path}")
def get_schema(
    url: str,
    time: datetime | None = None,
    storage: Storage = Depends(get_storage),
) -> Schema:
    if schema := storage.get_schema(url, time):
        return schema
    raise HTTPException(status_code=404)


@schema_router.put("/{url:path}")
def put_schema(
    url: str,
    schema: Schema,
    raw_schema_format: str | None = None,
    raw_schema: str | None = None,
    time: datetime | None = None,
    storage: Storage = Depends(get_storage),
):
    storage.put_schema(
        url,
        schema,
        raw_schema_format,
        raw_schema,
        time,
    )


@schema_router.delete("/{url:path}")
def delete_schema(
    url: str,
    time: datetime | None = None,
    storage: Storage = Depends(get_storage),
):
    storage.delete_schema(url, time)


fastapp = FastAPI()
fastapp.include_router(schema_router)
