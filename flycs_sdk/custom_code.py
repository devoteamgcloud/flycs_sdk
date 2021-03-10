"""Module containing class used to inject custom Airflow operator into pipelines definitions."""

from typing import List, Callable
import inspect


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

    # @classmethod
    # def from_dict(cls, d: dict):
    #     """[summary]

    #     :param d: [description]
    #     :type d: dict
    #     :return: [description]
    #     :rtype: [type]
    #     """
    #     return cls(entity=d["entity"], stage=d["stage"], name=d["query"])

    # def to_dict(self):
    #     return {
    #         "entity": self.entity,
    #         "stage": self.stage,
    #         "query": self.name,
    #     }


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
        self._ensure_builder_signature(operator_builder)

        self.name = name
        self.version = version
        self.operator_builder = operator_builder
        self.dependencies = dependencies or []

    def _ensure_builder_signature(self, f: Callable):
        signature = inspect.Signature.from_callable(f)
        if "dag" not in signature.parameters:
            raise WrongSignatureError(
                f"the builder function of the custom code {self.name}_{self.version} does not accept the mandatory `dag` argument"
            )
