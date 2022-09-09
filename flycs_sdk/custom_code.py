"""Module containing class used to inject custom Airflow operator into pipelines definitions."""

import inspect
from typing import Callable, Dict, List

from requirements.requirement import Requirement


class WrongSignatureError(TypeError):
    """Raised when passing a wrong operator builder function to  the CustomCode class."""

    pass


class Dependency:
    """Represent a pipeline operator dependency."""

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

    @classmethod
    def from_dict(cls, d):
        """Create a Dependency object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: Dependency
        :rtype: Dependency
        """
        return cls(entity=d["ENTITY"], stage=d["STAGE"], name=d["NAME"])

    def to_dict(self) -> dict:
        """
        Serialize the Dependency to a dictionary object.

        :return: the Dependency as a dictionary object.
        """
        return {
            "ENTITY": self.entity,
            "STAGE": self.stage,
            "NAME": self.name,
        }

    def __eq__(self, other):
        """Implement the __eq__ method."""
        return (
            self.name == other.name
            and self.entity == other.entity
            and self.stage == other.stage
        )

    def __hash__(self) -> int:
        """Implement the __eq__ method."""
        return hash(f"{self.name}{self.entity}{self.stage}")


class CustomCode:
    """Class representing a custom airflow operator to inject into an Airflow DAG."""

    def __init__(
        self,
        name: str,
        version: str,
        operator_builder: Callable,
        dependencies: List[Dependency] = None,
        requirements: List[str] = None,
        func_kwargs: Dict[str, object] = None,
        run_before_keyset: bool = False,
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
        :param requirements: list of python package required by this code, use the same format as normal python requirements.txt files.
                             These package will be installed on the composer instance.
        :type requirements: List[str]
        :param func_kwargs: List of kwargs arguments for a customer operator. E.g {"key_int" : 10}
        :type func_kwargs: Dict[str:object]
        :param run_before_keyset: overrides dependencies and runs the custom Operator before the keysets (keysets are Operators run before the Transformations when PII is activated)
        :type run_before_keyset: bool
        """
        self.name = name
        self.version = version
        self.operator_builder = operator_builder
        self.dependencies = dependencies or []
        self.requirements = requirements or []
        self.func_kwargs = func_kwargs or {}
        self.run_before_keyset = run_before_keyset or False

        self._ensure_builder_signature(operator_builder)
        self._validate_requirements()

    def _ensure_builder_signature(self, f: Callable):
        signature = inspect.Signature.from_callable(f)
        signature_parameters = list(signature.parameters.keys())
        if "dag" not in signature_parameters and "env" not in signature_parameters:
            raise WrongSignatureError(
                f"the builder function of the custom code {self.name}_{self.version} does not accept the mandatory ('dag', 'env', 'user') arguments"
            )

    def _validate_requirements(self):
        """
        Make sure the format used in the requirements list is valid.

        :raises ValueError: if format is not valid
        """
        for line in self.requirements:
            Requirement.parse(line)
