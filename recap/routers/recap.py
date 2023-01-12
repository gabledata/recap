from datetime import datetime
from fastapi import APIRouter, Body, Depends, HTTPException
from pathlib import PurePosixPath
from typing import Any, List
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog


router = APIRouter()

@router.get("/directory/{path:path}")
def list_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[str]:
    children = catalog.ls(PurePosixPath(path), as_of)
    if children:
        return children
    raise HTTPException(status_code=404)


@router.put("/directory/{path:path}")
def make_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.touch(PurePosixPath(path))


@router.delete("/directory/{path:path}")
def remove_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path))


@router.get("/metadata/{path:path}")
def read_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> dict[str, Any]:
    # TODO should probably return a 404 if we get None from storage
    metadata = catalog.read(PurePosixPath(path), as_of)
    if metadata:
        return metadata
    raise HTTPException(status_code=404)


@router.patch("/metadata/{path:path}")
def patch_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str,
    metadata: Any = Body(),
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.write(PurePosixPath(path), type, metadata)


@router.delete("/metadata/{path:path}")
def delete_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path), type)


@router.get("/search")
def query_search(
    query: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[dict[str, Any]]:
    return catalog.search(query, as_of)
