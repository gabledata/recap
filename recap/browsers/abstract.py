from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.paths import CatalogPath
from typing import Generator


class AbstractBrowser(ABC):
    """
    The abstract class for all browsers. Recap uses a browser abstraction to
    deal with different data infrastructure in a standard way.

    Browsers map infrastructure objects into a standard directory format. A
    different browser is used for each type of infrastructure.

    A DatabaseBrowser might list directories like this:

        /
        /databases
        /databases/postgresql
        /databases/postgresql/instances
        /databases/postgresql/instances/localhost
        /databases/postgresql/instances/localhost/schemas
        /databases/postgresql/instances/localhost/schemas/public
        /databases/postgresql/instances/localhost/schemas/public/tables
        /databases/postgresql/instances/localhost/schemas/public/tables/groups
        /databases/postgresql/instances/localhost/schemas/public/tables/users
    """

    @abstractmethod
    def children(self, path: PurePosixPath) -> list[CatalogPath] | None:
        """
        Given a path, returns its children. Using the example above,
        path=/databases/postgresql/instances/localhost/schemas/public/tables
        would return:

            [
                TablePath(
                    scheme='postgresl',
                    instance='localhost',
                    schema='public',
                    table='groups',
                ),
                TablePath(
                    scheme='postgresl',
                    instance='localhost',
                    schema='public',
                    table='users',
                ),
            ]
        """

        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractBrowser', None, None]:
        """
        Creates and returns a browser using the supplied config.
        """

        raise NotImplementedError
