from abc import ABC, abstractmethod
from recap.paths import CatalogPath


class AbstractBrowser(ABC):
    """
    The abstract class for all browsers. Recap uses a browser abstraction to
    deal with different data infrastructure in a standard way.

    Browsers map infrastructure objects into a directory format. A different
    browser is used for each type of infrastructure.

    A DatabaseBrowser might list directories like this:

        /schemas
        /schemas/public
        /schemas/public/tables
        /schemas/public/tables/groups
        /schemas/public/tables/users
    """

    @abstractmethod
    def children(self, path: str) -> list[CatalogPath] | None:
        """
        Given a path, returns its children. Using the example above,
        path=/schemas/public/tables would return:

            [
                TablePath(
                    schema='public',
                    table='groups',
                ),
                TablePath(
                    schema='public',
                    table='users',
                ),
            ]

        The path parameter is relative; it does not include the browser's root.
        """

        raise NotImplementedError

    @abstractmethod
    def root(self) -> CatalogPath:
        """
        The root path for this browser. The root path is prefixed to all
        paths when persisting metdata to a catalog.

        Using the example above, root might return something like:

            /databases/postgresql/instances/prd-db

        Thus, the full catalog path for the `users` table would be:

            /databases/postgresql/instances/prd-db/schemas/public/tables/users
        """
        raise NotImplementedError
