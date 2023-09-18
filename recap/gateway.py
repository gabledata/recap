from fastapi import FastAPI, HTTPException, Request

from recap import commands

app = FastAPI()

FORMAT_MAP = {
    "application/schema+json": commands.SchemaFormat.json,
    "application/avro+json": commands.SchemaFormat.avro,
    "application/x-protobuf": commands.SchemaFormat.protobuf,
    "application/x-recap": commands.SchemaFormat.recap,
}


@app.get("/ls/{url:path}")
async def ls(url: str | None = None) -> list[str]:
    """
    List the children of a URL.
    """

    children = commands.ls(url)
    if children is not None:
        return children
    raise HTTPException(status_code=404, detail="URL not found")


@app.get("/schema/{url:path}")
async def schema(url: str, request: Request):
    """
    Get the schema of a URL.
    """

    content_type = request.headers.get("content-type") or "application/x-recap"
    if format := FORMAT_MAP.get(content_type):
        return commands.schema(url, format)
    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported content type: {content_type}",
        )
