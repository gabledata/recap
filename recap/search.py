import pyjq
from .storage.abstract import AbstractStorage
from pathlib import PosixPath
from typing import List, Any

class JqSearch:
    def __init__(self, storage: AbstractStorage):
        self.storage = storage

    def list(self, path: str):
        return self.storage.list(path)

    def read(
        self,
        path: str,
    ) -> dict[str, Any]:
        metadata_path = PosixPath(path, 'metadata')
        metadata_types = self.storage.list(str(metadata_path)) or []
        aggregated_metadata = {}
        for type in metadata_types:
            metadata = self.storage.get_metadata(
                path,
                type,
            )
            aggregated_metadata[type] = metadata

        components_doc = self._assemble_path_components(PosixPath(path))

        return components_doc | {
            "metadata": aggregated_metadata,
        }

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []
        path_stack = [PosixPath('/')]
        while path_stack:
            path = path_stack.pop()

            if path.name == 'metadata':
                doc = self.read(str(path.parent))
                matches = pyjq.first(query, doc)
                if matches:
                    results.append(doc)
            else:
                children = self.storage.list(str(path)) or []
                children = map(lambda c: PosixPath(path, c), children)
                path_stack.extend(children)

        return results

    def _assemble_path_components(self, path: PosixPath) -> dict[str, str]:
        components_doc = {}
        path_parts = list(path.parts)

        # Get rid of leading separator if it's there
        if path_parts[0] == '/':
            path_parts = path_parts[1:]

        # Path format is always something like:
        #     /databases/<infra>/instances/<myinstance>/etc
        # So split the path up and pop off values and keys to add to the doc.
        # TODO singularize plural keys for user convenience
        while path_parts:
            v = path_parts.pop()
            k = path_parts.pop()
            components_doc[k] = v

        return components_doc
