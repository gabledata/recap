from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pathlib import PurePosixPath
from typing import List
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog


router = APIRouter(
    prefix="/directory"
)


@router.get("/{path:path}")
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


@router.put("/{path:path}")
def make_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.touch(PurePosixPath(path))


@router.delete("/{path:path}")
def remove_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path))
