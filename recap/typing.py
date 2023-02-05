from pydantic import BaseModel, create_model
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers import AbstractBrowser
from recap.paths import CatalogPath
from recap.plugins import load_analyzer_plugins
from types import ModuleType
from typing import Callable, get_type_hints, TypeVar


T = TypeVar("T")


class Inspector:
    def _method_types(
        self,
        param_name: str,
        method: Callable,
        base_type: type[T],
    ) -> list[type[T]]:
        """
        Given a method and a param name, look for all types in the param that
        extend base_type. Given this method:

            def foo() -> RootPath | DatabaseRootPath:
                ...

        Calling:

            _method_types('return', foo, CatalogPath)

        Would return [RootPath, DatabaseRootPath]. The method also handles list
        and dict args, so given:

            def bar(paths: list[SchemaPath | SchemasPath]):
                ...

        Calling:

            _method_types('paths', bar, CatalogPath)

        Would return [SchemaPath, SchemasPath].

        :param param_name: Param name as defined in get_type_hints response.
        :param method: Callable to inspect.
        :param base_type: Only return types that sublcass this type.
        """

        method_types = []
        type_hints = get_type_hints(method)
        if param_type_hints := type_hints.get(param_name):
            type_stack = [param_type_hints]
            while type_stack:
                child_type = type_stack.pop()
                try:  # EAFP
                    # Not all types are classes, so issubclass will barf sometimes
                    if issubclass(child_type, base_type):
                        method_types.append(child_type)
                except:
                    pass
                if hasattr(child_type, "__args__"):
                    type_stack.extend(
                        child_type.__args__
                    )  # pyright: ignore [reportGeneralTypeIssues]
        return method_types

    def _method_type(
        self, param_name: str, method: Callable, base_type: type[T]
    ) -> type[T] | None:
        if types := self._method_types(
            param_name,
            method,
            base_type,
        ):
            return types[0]
        return None


class AnalyzerInspector(Inspector):
    def __init__(self, analyzer_class: type[AbstractAnalyzer]):
        self.analyzer_class = analyzer_class

    def input_path_types(self) -> list[type[CatalogPath]]:
        """
        Get analyzer.analyze()'s input path= parameter type(s).
        """

        return self._method_types(
            "path",
            self.analyzer_class.analyze,
            CatalogPath,
        )

    def return_type(self) -> type[BaseMetadataModel] | None:
        """
        Get analyzer.analyze()'s return type.
        """

        return self._method_type(
            "return",
            self.analyzer_class.analyze,
            BaseMetadataModel,
        )


class BrowserInspector(Inspector):
    def __init__(self, browser_class: type[AbstractBrowser]):
        self.browser_class = browser_class

    def children_types(self) -> list[type[CatalogPath]]:
        """
        Get browser.children()'s return type(s)
        """

        return self._method_types(
            "return",
            self.browser_class.children,
            CatalogPath,
        )

    def root_type(self) -> type[CatalogPath] | None:
        """
        Get browser.root()'s return type.
        """

        return self._method_type(
            "return",
            self.browser_class.root,
            CatalogPath,
        )


class ModuleInspector:
    def __init__(self, module: ModuleType):
        self.module = module

    def browser_type(self) -> type[AbstractBrowser] | None:
        """
        Get the return type for module.create_browser()
        """

        browser_module_return = get_type_hints(
            self.module.create_browser,
        ).get("return")
        if browser_module_return:
            # browser_module_return is Generator[SomeBrowserType, None, None].
            # __args__[0] gets SomeBrowserType from the Generator type.
            return browser_module_return.__args__[0]
        return None

    def analyzer_type(self) -> type[AbstractAnalyzer] | None:
        """
        Get the return type for module.create_analyzer()
        """

        analyzer_module_return = get_type_hints(
            self.module.create_analyzer,
        ).get("return")
        if analyzer_module_return:
            # analyzer_module_return is Generator[SomeAnalyzerType, None, None].
            # __args__[0] gets SomeAnalyzerType from the Generator type.
            return analyzer_module_return.__args__[0]
        return None


def get_analyzers_for_path(
    path_class: type[CatalogPath],
) -> list[type[AbstractAnalyzer]]:
    analyzers = []
    for analyzer_module in load_analyzer_plugins().values():
        module_inspector = ModuleInspector(analyzer_module)
        if analyzer_class := module_inspector.analyzer_type():
            analyzer_inspector = AnalyzerInspector(analyzer_class)
            analyzer_input_paths = analyzer_inspector.input_path_types()
            if path_class in analyzer_input_paths:
                analyzers.append(analyzer_class)
    return analyzers


def get_pydantic_model_for_path(
    path_class: type[CatalogPath],
    name: str | None = None,
) -> type[BaseModel] | None:
    """
    Build a Pydantic model that combines all metadata models from analyzers
    compatible with `path_class`. Given:

        def analyze(path: SchemaPath) -> SomeSchemaMetadata:
            ...
        def analyze(path: SchemaPath) -> SomeOtherSchemaMetadata:
            ...

    Calling `get_pydantic_model_for_path(SchemaPath)` would return:

        class SchemaPathMetadata:
            some.schema: SomeSchemaMetadata
            some.other.schema: SomeOtherSchemaMetadata

    Where "some.schema" is SomeSchemaMetadata.key() and "some.other.schema" is
    SomeOtherSchemaMetadata.key().
    """

    path_attrs = {}
    for analyzer_class in get_analyzers_for_path(path_class):
        analyzer_inspector = AnalyzerInspector(analyzer_class)
        if analyzer_return_model := analyzer_inspector.return_type():
            path_attrs[analyzer_return_model.key()] = (analyzer_return_model, None)
    return (
        create_model(
            name or f"{path_class.__name__}Metadata",
            **path_attrs,
        )
        if path_attrs
        else None
    )
