"""Module containing class used to inject custom Airflow operator into pipelines definitions."""

import ast
import inspect
import textwrap
from typing import Callable, List


class WrongSignatureError(TypeError):
    """Raised when passing a wrong operator builder function to  the CustomCode class."""

    pass


class Dependency:
    """Represent a pipeline operator dependencyself."""

    def __init__(self, entity: str, stage: str, name: str):
        """Create a Dependency object.

        :param entity: name of the dependant entity
        :type entity: str
        :param stage: name of the dependant  stage
        :type stage: str
        :param name: name of the dependant query
        :type name: str
        """
        self.entity = entity
        self.stage = stage
        self.name = name


class CustomCode:
    """Class representing a custom airflow operator to inject into an Airflow DAG."""

    def __init__(
        self,
        name: str,
        version: str,
        operator_builder: Callable,
        dependencies: List[Dependency] = None,
    ):
        """Represent a custom Airflow code that needs to be injected into a DAG.

        :param name: name of this operation, the name is used to identify the custom code and allow to depend on it
        :type name: str
        :param version: version of this operation, the version is also used to identity the custom code and allow to depend on it
        :type version: str
        :param operator_builder: a function that accepts a `dag` argument and return an airflow operator
        :type operator_builder: Callable
        :param dependencies: list of dependencies for this operation.
                             The dependencies are used to define where in the DAG this operation should be inserted, defaults to None
        :type dependencies: List[Dependency], optional
        """
        self.name = name
        self.version = version
        self.operator_builder = operator_builder
        self.dependencies = dependencies or []

        self._ensure_builder_signature(operator_builder)

    @property
    def imported_modules(self) -> List[str]:
        """Return a list of module that are imported build the operator_builder code."""
        source = textwrap.dedent(inspect.getsource(self.operator_builder))
        modules = []
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ImportFrom):
                if not node.names[0].asname:  # excluding the 'as' part of import
                    modules.append(node.module)
            elif (
                isinstance(node, ast.Import) and not node.names[0].asname
            ):  # excluding the 'as' part of import
                modules.append(node.names[0].name)
        return modules

    def _ensure_builder_signature(self, f: Callable):
        signature = inspect.Signature.from_callable(f)
        if ["dag", "env"] != list(signature.parameters.keys()):
            raise WrongSignatureError(
                f"the builder function of the custom code {self.name}_{self.version} does not accept the mandatory ('dag', 'env', 'user') arguments"
            )
