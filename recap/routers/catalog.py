from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from recap.catalogs.abstract import AbstractCatalog
from recap.schemas.schema import Schema
from recap.server import get_catalog

router = APIRouter(
    prefix="/catalog",
)


@router.get("/urls/{url:path}")
def get_children(
    url: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[str]:
    children = catalog.children(url, time)
    if children is not None:
        return children
    raise HTTPException(status_code=404)


@router.put("/urls/{url:path}")
def put_url(
    url: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.add(url)


@router.delete("/urls/{url:path}")
def delete_url(
    url: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    return catalog.remove(url)


@router.put("/schemas/{url:path}")
def write_schema(
    url: str,
    schema: Schema,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.add(url, schema)


@router.get("/schemas/{url:path}")
def get_schema(
    url: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> Schema:
    if metadata := catalog.read(url, Schema, time=time):
        return metadata
    raise HTTPException(status_code=404)


@router.delete("/schemas/{url:path}")
def delete_schema(
    url: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.remove(url, Schema)


@router.get("/schemas")
def search_schema(
    query: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[Schema]:
    return catalog.search(query, Schema, time)
