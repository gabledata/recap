from typing import Annotated

import typer
from rich import print_json

from recap import commands
from recap.types import to_dict

app = typer.Typer()


@app.command()
def ls(path: Annotated[str, typer.Argument(help="Path to list children of.")] = "/"):
    """
    List the children of a path.
    """

    if children := commands.ls(path):
        print_json(data=children)


@app.command()
def schema(path: Annotated[str, typer.Argument(help="Path to get schema of.")]):
    """
    Get the schema of a path.
    """

    if recap_struct := commands.schema(path):
        print_json(data=to_dict(recap_struct))
