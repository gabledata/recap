import pyjq
from .abstract import AbstractSearch
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.storage.abstract import AbstractStorage
from typing import List, Any, Generator


class JqSearch(AbstractSearch):
    def __init__(
        self,
        storage: AbstractStorage,
    ):
        self.storage = storage

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []
        path_stack = [PurePosixPath('/')]

        while path_stack:
            path = path_stack.pop()
            doc = self.storage.read(PurePosixPath(path)) or {}

            # If the doc matches the query, add it to the results
            if pyjq.first(query, doc):
                results.append(doc)

            # Now add any children to the stack for processing
            children = self.storage.ls(path) or []
            children = map(lambda c: PurePosixPath(path, c), children)
            path_stack.extend(children)

        return results


@contextmanager
def open(
    storage: AbstractStorage,
    **config
) -> Generator[JqSearch, None, None]:
    yield JqSearch(storage)
