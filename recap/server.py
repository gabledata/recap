from datetime import datetime
from typing import Generator

from fastapi import APIRouter, Depends, FastAPI, HTTPException

from recap.schema.models import Type
from recap.storage import create_storage
from recap.storage.abstract import AbstractStorage, Direction

storage_router = APIRouter(
    prefix="/storage",
)


def get_storage() -> Generator[AbstractStorage, None, None]:
    yield create_storage()


@storage_router.get("/{url:path}/metadata/schema")
def schema(
    url: str,
    time: datetime | None = None,
    storage: AbstractStorage = Depends(get_storage),
) -> Type:
    if schema := storage.metadata(url, Type, time):
        return schema
    raise HTTPException(status_code=404)


@storage_router.get("/{url:path}/links/{relationship}")
def links(
    url: str,
    relationship: str,
    time: datetime | None = None,
    direction_type: str | None = None,
    storage: AbstractStorage = Depends(get_storage),
) -> list[str]:
    direction = (
        Direction.FROM
        if not direction_type or direction_type.lower() == "from"
        else Direction.TO
    )
    if links := storage.links(url, relationship, time, direction):
        return links
    raise HTTPException(status_code=404)


@storage_router.get("/search/schema")
def search(
    query: str,
    time: datetime | None = None,
    storage: AbstractStorage = Depends(get_storage),
) -> list[Type]:
    return storage.search(query, Type, time)


@storage_router.put("/{url:path}/metadata/schema")
def write(
    url: str,
    schema: Type,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.write(url, schema)


@storage_router.post("/{url:path}/links/{relationship}")
def link(
    url: str,
    relationship: str,
    other_url: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.link(url, relationship, other_url)


@storage_router.delete("/{url:path}/links/{relationship}")
def unlink(
    url: str,
    relationship: str,
    other_url: str,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.unlink(url, relationship, other_url)


fastapp = FastAPI()
fastapp.include_router(storage_router)
