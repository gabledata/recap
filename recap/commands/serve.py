import typer
from recap.config import settings
from recap.logging import setup_logging
from recap.server import fastapp


app = typer.Typer()


@app.command()
def serve():
    """
    Starts Recap's HTTP/JSON API server.
    """

    import uvicorn

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings('server.uvicorn', {}),
    )
