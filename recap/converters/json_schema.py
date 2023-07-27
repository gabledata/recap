from __future__ import annotations

import json
import re
from typing import Callable
from urllib.parse import urlparse

import httpx
from referencing import Registry, Resource, jsonschema, retrieval

from recap.types import (
    BoolType,
    FloatType,
    IntType,
    ListType,
    NullType,
    ProxyType,
    RecapType,
    RecapTypeRegistry,
    StringType,
    StructType,
)

AliasStrategy = Callable[[str], str]


class JSONSchemaConverter:
    def __init__(self, json_registry: Registry | None = None):
        self.registry = RecapTypeRegistry()
        self.json_registry = json_registry or Registry(
            retrieve=JSONSchemaConverter.httpx_retrieve
        )

    def to_recap(
        self,
        json_schema_str: str,
        alias_strategy: AliasStrategy | None = None,
    ) -> StructType:
        alias_strategy = alias_strategy or JSONSchemaConverter.urn_to_alias
        json_schema = json.loads(json_schema_str)
        recap_schema = self._parse(json_schema, alias_strategy)
        if not isinstance(recap_schema, StructType):
            raise ValueError("JSON schema must be an object")
        return recap_schema

    def _parse(
        self,
        json_schema: dict,
        alias_strategy: AliasStrategy,
    ) -> RecapType:
        extra_attrs = {}
        if "description" in json_schema:
            extra_attrs["doc"] = json_schema["description"]
        if "default" in json_schema:
            extra_attrs["default"] = json_schema["default"]
        if "$id" in json_schema:
            resource = Resource.from_contents(json_schema, jsonschema.DRAFT202012)
            resource_id = resource.id()
            assert resource_id is not None, f"$id must be set for {json_schema}"
            self.json_registry = resource @ self.json_registry
            extra_attrs["alias"] = alias_strategy(resource_id)

        match json_schema:
            case {"type": "object", "properties": properties}:
                fields = []
                for name, prop in properties.items():
                    field = self._parse(prop, alias_strategy)
                    if name not in json_schema.get("required", []):
                        field = field.make_nullable()
                    field.extra_attrs["name"] = name
                    fields.append(field)
                return StructType(fields, **extra_attrs)
            case {"type": "array", "items": items}:
                values = self._parse(items, alias_strategy)
                return ListType(values, **extra_attrs)
            case {"type": "string", "format": "date"}:
                return StringType(
                    bytes_=9_223_372_036_854_775_807,
                    logical="org.iso.8601.Date",
                    **extra_attrs,
                )
            case {"type": "string", "format": "date-time"}:
                return StringType(
                    bytes_=9_223_372_036_854_775_807,
                    logical="org.iso.8601.DateTime",
                    **extra_attrs,
                )
            case {"type": "string", "format": "time"}:
                return StringType(
                    bytes_=9_223_372_036_854_775_807,
                    logical="org.iso.8601.Time",
                    **extra_attrs,
                )
            case {"type": "string"}:
                return StringType(bytes_=9_223_372_036_854_775_807, **extra_attrs)
            case {"type": "number"}:
                return FloatType(bits=64, **extra_attrs)
            case {"type": "integer"}:
                return IntType(bits=32, signed=True, **extra_attrs)
            case {"type": "boolean"}:
                return BoolType(**extra_attrs)
            case {"type": "null"}:
                return NullType(**extra_attrs)
            case {"$ref": str(urn)} | {"$dynamicRef": str(urn)}:
                try:
                    # If no KeyError is thrown here, we've already crawled this,
                    # so we can just return a ProxyType referencing the original.
                    resource = self.json_registry[urn]
                    assert (
                        resource.id() is not None
                    ), f"$id must be set for {resource.contents}"
                    alias = alias_strategy(str(resource.id()))
                    return ProxyType(alias, self.registry, **extra_attrs)
                except KeyError:
                    # We haven't crawled this type yet, so crawl and define the
                    # type as a normal Recap type with an alias set.
                    retrieved = self.json_registry.get_or_retrieve(urn)
                    resource = retrieved.value
                    self.json_registry = (
                        retrieved.registry @ self.json_registry
                    )  # pyright: ignore[reportGeneralTypeIssues]
                    return self._parse(resource.contents, alias_strategy)
            case _:
                raise ValueError(f"Unsupported JSON schema: {json_schema}")

    @staticmethod
    def urn_to_alias(urn: str) -> str:
        """
        Convert a URN to a dotted alias name. The following rules are used:

        - The domain is reversed, e.g. "example.com" becomes "com.example"
        - The path is converted to lowercase and special characters are replaced
          with dots, e.g. "/foo/bar" becomes "foo.bar"
        - The domain and path are combined, e.g. "com.example/foo.bar"

        It's possible that this function will generate the same alias for
        different URNs. If this happens, you can provide a custom alias
        strategy to the `to_recap` method.

        :param urn: The URN to convert
        :return: The dotted alias
        """

        # Parse the URN/URL
        parsed = urlparse(urn)

        # Reverse domain
        domain_parts = parsed.netloc.split(".")
        domain_parts.reverse()
        domain = ".".join(domain_parts)

        # Replace special characters in the path, convert to lowercase
        path = parsed.path + parsed.fragment
        path = re.sub(r"[^a-zA-Z0-9]", ".", path)
        path = path.lower()

        # Combine domain and path
        dotted_type = domain
        if path and path != ".":
            dotted_type += "." + path.strip(".")

        return dotted_type

    @staticmethod
    @retrieval.to_cached_resource()
    def httpx_retrieve(uri: str):
        return httpx.get(uri).text
