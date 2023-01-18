from pydantic import BaseModel, create_model
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers import AbstractBrowser
from recap.paths import CatalogPath
from recap.plugins import load_analyzer_plugins
from types import ModuleType
from typing import get_type_hints


class AnalyzerInspector:
    def __init__(self, analyzer_class: type[AbstractAnalyzer]):
        self.analyzer_class = analyzer_class

    def input_path_types(self) -> list[type[CatalogPath]]:
        input_paths = []
        analyze_method = self.analyzer_class.analyze
        if analyze_paths := get_type_hints(analyze_method).get('path'):
            type_stack = [analyze_paths]
            while type_stack:
                child_type = type_stack.pop()
                try: # EAFP
                    # Not all types are classes, so issubclass will barf sometimes
                    if issubclass(child_type, CatalogPath):
                        input_paths.append(child_type)
                except:
                    pass
                if hasattr(child_type, '__args__'):
                    type_stack.extend(child_type.__args__) # pyright: ignore [reportGeneralTypeIssues]
        return input_paths

    def return_type(self) -> type[BaseMetadataModel] | None:
        analyze_method = self.analyzer_class.analyze
        if return_model := get_type_hints(analyze_method).get('return'):
            type_stack = [return_model]
            while type_stack:
                child_type = type_stack.pop()
                try: # EAFP
                    # Not all types are classes, so issubclass will barf sometimes
                    if issubclass(child_type, BaseMetadataModel):
                        return child_type
                except:
                    pass
                if hasattr(child_type, '__args__'):
                    type_stack.extend(child_type.__args__) # pyright: ignore [reportGeneralTypeIssues]
        return None


class BrowserInspector:
    def __init__(self, browser_class: type[AbstractBrowser]):
        self.browser_class = browser_class

    def children_types(self) -> list[type[CatalogPath]]:
        return_paths = []
        children_method = self.browser_class.children
        children_type_hints = get_type_hints(children_method)
        if children_returns := children_type_hints.get('return'):
            type_stack = [children_returns]
            while type_stack:
                child_type = type_stack.pop()
                try: # EAFP
                    # Not all types are classes, so issubclass will barf sometimes
                    if issubclass(child_type, CatalogPath):
                        return_paths.append(child_type)
                except:
                    pass
                if hasattr(child_type, '__args__'):
                    type_stack.extend(child_type.__args__) # pyright: ignore [reportGeneralTypeIssues]
        return return_paths

    def root_type(self) -> type[CatalogPath] | None:
        root_method = self.browser_class.root
        if return_model := get_type_hints(root_method).get('return'):
            type_stack = [return_model]
            while type_stack:
                child_type = type_stack.pop()
                try: # EAFP
                    # Not all types are classes, so issubclass will barf sometimes
                    if issubclass(child_type, CatalogPath):
                        return child_type
                except:
                    pass
                if hasattr(child_type, '__args__'):
                    type_stack.extend(child_type.__args__) # pyright: ignore [reportGeneralTypeIssues]
        return None


class ModuleInspector:
    def __init__(self, module: ModuleType):
        self.module = module

    def browser_type(self) -> type[AbstractBrowser] | None:
        browser_module_return = get_type_hints(
            self.module.create_browser,
        ).get('return')
        if browser_module_return:
            # browser_module_return is Generator[SomeBrowserType, None, None].
            # __args__[0] gets SomeBrowserType from the Generator type.
            return browser_module_return.__args__[0]
        return None

    def analyzer_type(self) -> type[AbstractAnalyzer] | None:
        analyzer_module_return = get_type_hints(
            self.module.create_analyzer,
        ).get('return')
        if analyzer_module_return:
            # analyzer_module_return is Generator[SomeAnalyzerType, None, None].
            # __args__[0] gets SomeAnalyzerType from the Generator type.
            return analyzer_module_return.__args__[0]
        return None


def get_analyzers_for_path(path_class: type[CatalogPath]) -> list[type[AbstractAnalyzer]]:
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
    path_attrs = {}
    for analyzer_class in get_analyzers_for_path(path_class):
        analyzer_inspector = AnalyzerInspector(analyzer_class)
        if analyzer_return_model := analyzer_inspector.return_type():
            path_attrs[analyzer_return_model.key()] = (analyzer_return_model, None)
    return create_model(
        name or f"{path_class.__name__}Metadata",
        **path_attrs,
    ) if path_attrs else None
