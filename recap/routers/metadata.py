from datetime import datetime
from fastapi import APIRouter, Body, Depends, HTTPException
from typing import Any
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog


router = APIRouter(
    prefix="/metadata"
)


@router.get("/{path:path}")
def read_metadata(
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> dict[str, Any]:
    metadata = catalog.read(path, as_of)
    if metadata:
        return metadata
    raise HTTPException(status_code=404)


@router.patch("/{path:path}")
def patch_metadata(
    path: str,
    type: str,
    metadata: Any = Body(),
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.write(path, type, metadata)


@router.delete("/{path:path}")
def delete_metadata(
    path: str,
    type: str | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(path, type)
