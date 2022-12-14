import pyjq
from .storage.abstract import AbstractStorage
from typing import List, Any

class JqSearch:
    def __init__(self, storage: AbstractStorage):
        self.storage = storage

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []
        for infra in self.storage.list_infra():
            for instance in self.storage.list_instances(infra):
                for schema in self.storage.list_schemas(infra, instance):
                    for table in self.storage.list_tables(infra, instance, schema):
                        doc = self._assemble_doc(infra, instance, schema, table=table)
                        matches = pyjq.first(query, doc)
                        if matches:
                            results.append(doc)
                    for view in self.storage.list_views(infra, instance, schema):
                        doc = self._assemble_doc(infra, instance, schema, view=view)
                        matches = pyjq.first(query, doc)
                        if matches:
                            results.append(doc)
        return results

    def _assemble_doc(
        self,
        infra: str,
        instance: str,
        schema: str,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, Any]:
        table_or_view_key = 'table' if table else 'view'
        aggregated_metadata = {}
        metadata_types = self.storage.list_metadata(
            infra,
            instance,
            schema,
            table,
            view
        ) or []
        for type in metadata_types:
            metadata = self.storage.get_metadata(
                infra,
                instance,
                type,
                schema,
                table,
                view
            )
            aggregated_metadata[type] = metadata

        return {
            "infrastructure": infra,
            "instance": instance,
            "schema": schema,
            table_or_view_key: table or view,
            "metadata": aggregated_metadata,
        }
