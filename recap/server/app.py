from fastapi import FastAPI

from recap.server import gateway, registry

app = FastAPI()
app.include_router(gateway.router)
app.include_router(registry.router)
