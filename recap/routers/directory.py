from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog


router = APIRouter(
    prefix="/directory"
)


@router.get("/{path:path}")
def list_directory(
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[str]:
    children = catalog.ls(path, as_of)
    if children:
        return children
    raise HTTPException(status_code=404)


@router.put("/{path:path}")
def make_directory(
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.touch(path)


@router.delete("/{path:path}")
def remove_directory(
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(path)
