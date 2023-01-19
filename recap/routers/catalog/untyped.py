from datetime import datetime
from fastapi import APIRouter, Body, Depends, HTTPException
from pathlib import PurePosixPath
from pydantic import BaseModel
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog
from typing import Any


router = APIRouter(
    prefix="/catalog"
)


@router.get("/{path:path}/metadata")
def read_metadata(
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> dict[str, Any]:
    print(clean_path(path))
    metadata = catalog.read(clean_path(path), as_of)
    if metadata:
        return metadata
    raise HTTPException(status_code=404)


@router.patch("/{path:path}/metadata")
def patch_metadata(
    path: str,
    metadata: dict[str, Any] = Body(),
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.write(clean_path(path), metadata, True)


@router.put("/{path:path}/metadata")
def put_metadata(
    path: str,
    metadata: BaseModel = Body(),
    catalog: AbstractCatalog = Depends(get_catalog),
):
    metadata_ = metadata.dict(
        by_alias=True,
        exclude_defaults=True,
        exclude_none=True,
        exclude_unset=True,
    )
    catalog.write(clean_path(path), metadata_)


@router.get("/{path:path}/children")
def list_children(
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[str]:
    children = catalog.ls(clean_path(path), as_of)
    if children:
        return children
    raise HTTPException(status_code=404)


@router.delete("/{path:path}")
def remove_directory(
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(clean_path(path))


@router.get("")
def query_search(
    query: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[dict[str, Any]]:
    return catalog.search(query, as_of)


def clean_path(path: str) -> str:
    return str(PurePosixPath('/', path))
