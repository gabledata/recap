from pathlib import PurePosixPath
from pydantic import BaseModel
from typing import ClassVar, Optional, Type
from starlette.routing import compile_path


class CatalogPath(BaseModel):
    """
    Represents a path in the data catalog.
    """

    template: ClassVar[str]
    """
    A list of templated paths that represent the various locations of this path
    type. The template format uses Starlette's simplified URI style. For
    example:

        /databases/{scheme}/instances/{instance}/schemas/{schema}
    """

    def name(self) -> str:
        return PurePosixPath(str(self)).name

    def __str__(self) -> str:
        """
        Find a template format that has parameters compatible with this
        instance. If one is found, fill it in with this instance's
        attributes and return it. For example, a template:

            /databases/{scheme}/instances/{instance}/schemas/{schema}

        Would return:

            /databases/postgresql/instances/localhost/schemas/public

        If the instance had:

            SomeCatalogPath(
                scheme='postgresql',
                instance='localhost',
                schema='public']
            )
        """
        # TODO Should use Starlette formatting so typed variables
        # (e.g. {path:path} or {name:str}) are handled properly.
        return self.template.format(**self.dict(
                by_alias=True,
                exclude_none=True,
                exclude_unset=True,
            ))

    @classmethod
    def from_path(cls, path: str) -> Optional['CatalogPath']:
        """
        Construct this model from a path if it matches the template.
        """
        path_str = str(path)
        template_regex, _, _ = compile_path(cls.template)
        if match := template_regex.match(path_str):
            return cls(**match.groupdict())
        return None


class RootPath(CatalogPath):
    """
    Canonical root path for the catalog.
    """
    template = '/'


def create_catalog_path(
    path: str,
    *types: Type[CatalogPath],
) -> CatalogPath | None:
    """
    Given a collection of CatalogPath types, find one that has a template that
    matches the provided path. If a matching path is found, instantiate the
    CatalogPath type and return it.
    """
    for type_ in types + (RootPath, ):
        if catalog_path := type_.from_path(path):
            return catalog_path
    return None
