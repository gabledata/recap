from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog

router = APIRouter(
    prefix="/catalog",
)


@router.get("/directory/{path:path}")
def directory(
    path: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[str] | None:
    return catalog.ls(
        path=path,
        time=time,
    )


@router.patch("/metadata/{path:path}")
@router.put("/metadata/{path:path}")
def write_metadata(
    path: str,
    metadata: dict[str, Any],
    request: Request,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.write(
        path=path,
        metadata=metadata,
        patch=request.method.upper() == "PATCH",
    )


@router.get("/metadata/{path:path}")
def read_metadata(
    path: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> dict[str, Any] | None:
    return catalog.read(path=path, time=time)


@router.delete("/metadata/{path:path}")
def delete_metadata(
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(path)


@router.get("/search")
def search(
    query: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[dict[str, Any]]:
    return catalog.search(query, time)
