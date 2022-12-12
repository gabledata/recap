import typer

app = typer.Typer()


@app.command()
def api():
    import uvicorn
    uvicorn.run(
        "recap.api:app",
        host="0.0.0.0",
        port=8000,
    )


@app.command()
def crawler(recap_url, crawl_url):
    import httpx
    import sqlalchemy as sa
    from .crawler.db import Instance, Crawler
    from .storage import RecapTableStorage
    with httpx.Client(base_url=recap_url) as client:
        # TODO read crawler configs from a TOML file
        engine = sa.create_engine(crawl_url)
        instance = Instance(engine)
        storage = RecapTableStorage(
            'postgresql',
            'sticker_space_dev',
            client
        )
        crawler = Crawler(storage, instance)
        crawler.crawl()


if __name__ == "__main__":
    app()
