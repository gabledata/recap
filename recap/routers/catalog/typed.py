import inspect
from datetime import datetime
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel
from recap.catalogs.abstract import AbstractCatalog
from recap.paths import CatalogPath
from recap.plugins import load_browser_plugins
from recap.server import get_catalog
from recap.typing import (
    BrowserInspector,
    ModuleInspector,
    get_pydantic_model_for_path,
)
from typing import Callable


router = APIRouter(
    prefix="/catalog"
)


def add_route(
    router: APIRouter,
    endpoint: Callable,
    method: str,
    metadata_class: type[BaseModel],
    browser_root_path: type[CatalogPath],
    child_path: type[CatalogPath],
):
    dynamic_params = []
    path_fields = browser_root_path.__fields__ | child_path.__fields__
    endpoint_signature = inspect.signature(endpoint)
    endpoint_existing_params = [
        p for p in endpoint_signature.parameters.values()
        if p.kind != inspect.Parameter.VAR_KEYWORD
    ]

    for field in path_fields.values():
        dynamic_params.append(
            inspect.Parameter(
                field.alias or field.name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=field.type_,
                default=field.default,
            )
        )

    endpoint.__signature__ = inspect.Signature(
        dynamic_params + endpoint_existing_params
    )

    router.add_api_route(
        path=browser_root_path.template + child_path.template + '/metadata',
        endpoint=endpoint,
        dependencies=[Depends(get_catalog)],
        response_model=metadata_class if method == 'GET' else None,
        methods=[method],
    )


def add_routes(
    router: APIRouter,
    metadata_class: type[BaseModel],
    browser_root_path: type[CatalogPath],
    child_path: type[CatalogPath],
):
    def read_metadata(
        as_of: datetime | None = None,
        catalog: AbstractCatalog = Depends(get_catalog),
        **kwargs,
    ) -> metadata_class:
        path = (browser_root_path.template + child_path.template).format(**kwargs)
        metadata = catalog.read(path, as_of)
        if metadata:
            return metadata_class.parse_obj(metadata)
        raise HTTPException(status_code=404)

    def put_metadata(
        metadata: metadata_class = Body(),
        catalog: AbstractCatalog = Depends(get_catalog),
        **kwargs,
    ):
        path = (browser_root_path.template + child_path.template).format(**kwargs)
        metadata_dict = metadata.dict(
            by_alias=True,
            exclude_defaults=True,
            exclude_none=True,
            exclude_unset=True,
        )
        catalog.write(path, metadata_dict)

    def patch_metadata(
        metadata: metadata_class = Body(),
        catalog: AbstractCatalog = Depends(get_catalog),
        **kwargs,
    ):
        path = (browser_root_path.template + child_path.template).format(**kwargs)
        metadata_dict = metadata.dict(
            by_alias=True,
            exclude_defaults=True,
            exclude_none=True,
            exclude_unset=True,
        )
        catalog.write(path, metadata_dict, True)

    add_route(
        router,
        read_metadata,
        'GET',
        metadata_class,
        browser_root_path,
        child_path,
    )

    add_route(
        router,
        put_metadata,
        'PUT',
        metadata_class,
        browser_root_path,
        child_path,
    )

    add_route(
        router,
        patch_metadata,
        'PATCH',
        metadata_class,
        browser_root_path,
        child_path,
    )


def add_metadata_paths(router: APIRouter):
    for browser_module in load_browser_plugins().values():
        module_inspector = ModuleInspector(browser_module)
        if browser_class := module_inspector.browser_type():
            browser_inspector = BrowserInspector(browser_class)
            root_path_type = browser_inspector.root_type()
            for child_path_class in browser_inspector.children_types():
                metadata_class = get_pydantic_model_for_path(child_path_class)
                if root_path_type and metadata_class:
                    add_routes(
                        router,
                        metadata_class,
                        root_path_type,
                        child_path_class,
                    )


add_metadata_paths(router)
