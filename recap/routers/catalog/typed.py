"""
This module dynamically adds all available browser paths to a router.

The module works using type hints. It inspects both the `children()` and
`analyze()` methods for each browser and analyzer. It then infers which
analyzers can analyze paths from each browser. The module uses this information
to construct Pydantic models that contain all compatible analyzers for each
path. It adds these paths and models to the router.
"""

import inspect
from datetime import datetime
from typing import Callable

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel

from recap.catalogs.abstract import AbstractCatalog
from recap.paths import CatalogPath
from recap.plugins import load_browser_plugins
from recap.server import get_catalog
from recap.typing import BrowserInspector, ModuleInspector, get_pydantic_model_for_path

router = APIRouter(
    prefix="/catalog",
)


def add_route(
    router: APIRouter,
    endpoint: Callable,
    method: str,
    browser_root_path_class: type[CatalogPath],
    child_path_class: type[CatalogPath],
    response_model: type[BaseModel] | None = None,
):
    """
    Adds a metadata endpoint to a router. The endpoint's signature is
    overwritten to include the fields from the root and child paths.

        root=/databases/{scheme}/instances/{name}
        child=/schemas/{schema}

    Would result in scheme, name, and schema parameters being added to the
    endpoint. This will cause FastAPI to pass these path parameters into the
    endpoint, which allows the *_metadata methods below to grab them in
    **kwargs.

    :param router: The router to add a route to.
    :param endpoint: The endpoint (method) to add to the router.
    :param method: GET, PUT, POST, PATCH, DELETE, and so on.
    :param browser_root_path_class: Browser's root path to be used in HTTP path.
    :param child_path_class: Child path relative to browser's root to be used in HTTP
        path.
    :param response_model: The response_model class for GET methods.
    """

    dynamic_params = []
    path_fields = browser_root_path_class.__fields__ | child_path_class.__fields__
    endpoint_signature = inspect.signature(endpoint)
    endpoint_existing_params = [
        p
        for p in endpoint_signature.parameters.values()
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

    metadata_path = (
        browser_root_path_class.template + child_path_class.template + "/metadata"
    )

    router.add_api_route(
        path=metadata_path,
        endpoint=endpoint,
        dependencies=[Depends(get_catalog)],
        response_model=response_model,
        methods=[method],
        # These only apply for GET, but no harm in including them for all.
        response_model_exclude_defaults=True,
        response_model_exclude_none=True,
        response_model_exclude_unset=True,
    )


def add_routes(
    router: APIRouter,
    metadata_class: type[BaseModel],
    browser_root_path_class: type[CatalogPath],
    child_path_class: type[CatalogPath],
):
    """
    Helper method that adds GET, PUT, and PATCH routes to a router for a given
    model/browser path combination.

    :param router: The router to add routes to.
    :param metadata_class: The Pydantic metadata model that represents metadata
        for the root/child path.
    :param browser_root_path_class: Browser's root path to be used in HTTP path.
    :param child_path_class: Child path relative to browser's root to be used in HTTP
        path.
    """

    def read_metadata(
        time: datetime | None = None,
        catalog: AbstractCatalog = Depends(get_catalog),
        **kwargs,
    ) -> metadata_class:
        browser_root_path = browser_root_path_class.parse_obj(kwargs)
        child_path = child_path_class.parse_obj(kwargs)
        path = str(browser_root_path) + str(child_path)
        metadata = catalog.read(path, time)
        if metadata:
            return metadata_class.parse_obj(metadata)
        raise HTTPException(status_code=404)

    def put_metadata(
        metadata: metadata_class = Body(),
        catalog: AbstractCatalog = Depends(get_catalog),
        **kwargs,
    ):
        browser_root_path = browser_root_path_class.parse_obj(kwargs)
        child_path = child_path_class.parse_obj(kwargs)
        path = str(browser_root_path) + str(child_path)
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
        browser_root_path = browser_root_path_class.parse_obj(kwargs)
        child_path = child_path_class.parse_obj(kwargs)
        path = str(browser_root_path) + str(child_path)
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
        "GET",
        browser_root_path_class,
        child_path_class,
        metadata_class,
    )

    add_route(
        router,
        put_metadata,
        "PUT",
        browser_root_path_class,
        child_path_class,
    )

    add_route(
        router,
        patch_metadata,
        "PATCH",
        browser_root_path_class,
        child_path_class,
    )


def add_metadata_paths(router: APIRouter):
    """
    Dynamically adds all browser paths to the router. `add_metadata_paths`
    looks at the `children()` method's return CatalogPaths for each browser,
    and gets all analyzers that analyze one or more of those paths. The method
    then gets the return Pydantic models for each analyze and combines them
    into an uber-Metadata model. A GET/PUT/PATCH endpoint is added for each of
    these models.

    :param router: Router to add metadata routes to.
    """

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
