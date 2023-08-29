from fastapi import FastAPI, HTTPException

from recap import commands
from recap.types import to_dict

app = FastAPI()


@app.get("/ls/{path:path}")
async def ls(path: str = "/") -> list[str]:
    """
    List the children of a path.
    """

    children = commands.ls(path)
    if children is not None:
        return children
    raise HTTPException(status_code=404, detail="Path not found")


@app.get("/schema/{path:path}")
async def schema(path: str) -> dict:
    """
    Get the schema of a path.
    """

    if recap_struct := commands.schema(path):
        recap_dict = to_dict(recap_struct)
        if not isinstance(recap_dict, dict):
            raise HTTPException(
                status_code=503,
                detail=f"Expected a schema dict, but got {type(recap_dict)}",
            )
        return recap_dict
    raise HTTPException(status_code=404, detail="Path not found")
