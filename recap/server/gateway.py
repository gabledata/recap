from fastapi import APIRouter, HTTPException, Request

from recap import commands

router = APIRouter(prefix="/gateway")


@router.get("/ls/{url:path}")
async def ls(url: str | None = None) -> list[str]:
    """
    List the children of a URL.
    """

    children = commands.ls(url)
    if children is not None:
        return children
    raise HTTPException(status_code=404, detail="URL not found")


@router.get("/schema/{url:path}")
async def schema(url: str, request: Request):
    """
    Get the schema of a URL.
    """

    content_type = request.headers.get("content-type") or "application/x-recap+json"
    if format := commands.FORMAT_MAP.get(content_type):
        return commands.schema(url, format)
    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported content type: {content_type}",
        )
