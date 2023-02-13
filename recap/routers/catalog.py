from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog

router = APIRouter(
    prefix="/catalog",
)


@router.get("/directory/{url:path}")
def directory(
    url: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[str] | None:
    return catalog.ls(
        url=url,
        time=time,
    )


@router.patch("/metadata/{url:path}")
@router.put("/metadata/{url:path}")
def write_metadata(
    url: str,
    metadata: dict[str, Any],
    request: Request,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.write(
        url=url,
        metadata=metadata,
        patch=request.method.upper() == "PATCH",
    )


@router.get("/metadata/{url:path}")
def read_metadata(
    url: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> dict[str, Any] | None:
    return catalog.read(url=url, time=time)


@router.delete("/metadata/{url:path}")
def delete_metadata(
    url: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(url)


@router.get("/search")
def search(
    query: str,
    time: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> list[dict[str, Any]]:
    return catalog.search(query, time)
