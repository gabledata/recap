from fastapi import FastAPI, HTTPException

from recap import commands
from recap.types import to_dict

app = FastAPI()


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
async def schema(url: str) -> dict:
    """
    Get the schema of a URL.
    """

    if recap_struct := commands.schema(url):
        recap_dict = to_dict(recap_struct)
        if not isinstance(recap_dict, dict):
            raise HTTPException(
                status_code=503,
                detail=f"Expected a schema dict, but got {type(recap_dict)}",
            )
        return recap_dict
    raise HTTPException(status_code=404, detail="URL not found")
