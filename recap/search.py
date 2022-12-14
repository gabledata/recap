import pyjq
from .storage.abstract import AbstractStorage
from pathlib import PurePosixPath
from typing import List, Any

class JqSearch:
    def __init__(self, storage: AbstractStorage):
        self.storage = storage

    def list(self, path: str):
        return self.storage.ls(PurePosixPath(path))

    def read(
        self,
        path: str,
    ) -> dict[str, Any]:
        return self.storage.read(PurePosixPath(path)) or {}

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []
        path_stack = [PurePosixPath('/')]

        while path_stack:
            path = path_stack.pop()
            doc = self.read(str(path))

            # If the doc matches the query, add it to the results
            if pyjq.first(query, doc):
                results.append(doc)

            # Now add any children to the stack for processing
            children = self.storage.ls(path) or []
            children = map(lambda c: PurePosixPath(path, c), children)
            path_stack.extend(children)

        return results
