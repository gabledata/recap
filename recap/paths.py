from pathlib import PurePosixPath
from pydantic import BaseModel
from typing import ClassVar, Optional, Type
from starlette.routing import compile_path


class CatalogPath(BaseModel):
    """
    Represents a path in the data catalog.
    """

    templates: ClassVar[list[PurePosixPath]]
    """
    A list of templated paths that represent the various locations of this path
    type. The template format uses Starlette's simplified URI style. For
    example:

        /databases/{scheme}/instances/{instance}/schemas/{schema}
    """

    def name(self) -> str:
        return self.path().parts[-1]

    def path(self) -> PurePosixPath:
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
        for template in self.__class__.templates:
            try:
                return PurePosixPath(
                    # TODO Should use Starlette formatting so typed variables
                    # (e.g. {path:path} or {name:str}) are handled properly.
                    str(template).format(**self.dict(
                        by_alias=True,
                        exclude_none=True,
                        exclude_unset=True,
                    ))
                )
            except KeyError:
                pass
        raise ValueError("No template is compatible with model variables")

    def __str__(self) -> str:
        return str(self.path())

    @classmethod
    def from_path(cls, path: PurePosixPath) -> Optional['CatalogPath']:
        """
        Construct this model from a PurePosixPath the path matches one of this
        model's templates.
        """
        path_str = str(path)
        for template in cls.templates:
            template_regex, _, _ = compile_path(str(template))
            if match := template_regex.match(path_str):
                return cls(**match.groupdict())
        return None


class RootPath(CatalogPath):
    """
    Canonical root path for the catalog.
    """
    templates = [PurePosixPath('/')]


def create_catalog_path(
    path: PurePosixPath,
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
