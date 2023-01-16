from fastapi import FastAPI
from typing import Generator
from . import catalogs, plugins
from recap.catalogs.abstract import AbstractCatalog
from recap.config import settings


DEFAULT_URL = 'http://localhost:8000'


fastapp = FastAPI()


def get_catalog() -> Generator[AbstractCatalog, None, None]:
    with catalogs.create_catalog(**settings('catalog', {})) as c:
        yield c


@fastapp.on_event("startup")
def load_plugins():
    router_plugins = plugins.load_router_plugins()
    for plugin_router in router_plugins.values():
        fastapp.include_router(plugin_router)
