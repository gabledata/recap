from datetime import datetime
from typing import Generator

from fastapi import APIRouter, Depends, FastAPI, HTTPException

from recap.catalog.storage import Storage, create_storage
from recap.models import Schema

schema_router = APIRouter(
    prefix="/schema",
)


def get_storage() -> Generator[Storage, None, None]:
    yield create_storage()


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
