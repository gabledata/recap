from abc import ABC, abstractmethod

from recap.url import URL


class AbstractBrowser(ABC):
    """
    The abstract class for all browsers. Recap uses a browser abstraction to
    deal with different data infrastructure in a standard way.

    Browsers map infrastructure objects into a directory format. A different
    browser is used for each type of infrastructure.

    A DatabaseBrowser might list directories like this:

        /
        /some_db
        /some_db/some_table
        /some_db/some_other_table
    """

    url: URL

    @abstractmethod
    def children(self, path: str) -> list[str] | None:
        """
        Given a path, returns its children. Using the example above,
        path=/some_db would return:

            [
                "some_table",
                "some_other_table"
            ]

        The path parameter is relative to the input path.

        :returns: List of child paths relative to input path. None if path
            doesn't exist.
        """

        raise NotImplementedError
