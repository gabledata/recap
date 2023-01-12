from datetime import datetime
from fastapi import APIRouter, Depends
from typing import Any, List
from recap.catalogs.abstract import AbstractCatalog
from recap.server import get_catalog


router = APIRouter(
    prefix="/search"
)


@router.get("")
def query_search(
    query: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[dict[str, Any]]:
    return catalog.search(query, as_of)
