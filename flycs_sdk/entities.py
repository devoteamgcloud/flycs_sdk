"""Module containing entity classes."""

from typing import Dict, List, Optional, Union
from .custom_code import CustomCode
from enum import Enum
from .transformations import Transformation
from .views import View


class ConflictingNameError(ValueError):
    """Raised when trying to insert a Transformation or View into an entities \
    that already contains a Transformation or View with the same name."""

    pass


class EntityKind(Enum):
    """This enumeration contains all the supported entity types."""

    VANILLA = "vanilla"
    DELTA_TRACKING = "delta_tracking"
    DATA_VAULT = "data_vault"


class Entity:
    """Class that serves as a version configuration for a logical subset of a Pipeline."""

    def __init__(
        self,
        name: str,
        version: str,
        kind: Optional[EntityKind] = None,
        stage_config: Optional[Dict[str, Dict[str, str]]] = None,
        custom_operators: Optional[Dict[str, List[CustomCode]]] = None,
    ):
        """
        Create an Entity object.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming this entity belongs to.
        :param kind: the kind of the entity
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        :param custom_operators: a dictionary with the name of the stage as key and a list
                                 of CustomCode objects allowing to inject custom Airflow operator
                                 into the pipeline as value
        :type custom_operators: list
        """
        self.name = name
        self.version = version
        self.kind = kind
        self.stage_config = stage_config or {}
        self.transformations = {}
        self.custom_operators = custom_operators or {}

    @classmethod
    def from_dict(cls, d: dict):
        """
        Create an Entity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: Entity
        :rtype: Entity
        """
        stage_config = {stage["name"]: stage["versions"] for stage in d["stage_config"]}
        return cls(
            name=d["name"],
            version=d["version"],
            kind=EntityKind(d["kind"]) if d.get("kind") is not None else None,
            stage_config=stage_config,
        )

    @property
    def stages(self):
        """Return a list of all the stages defined in this entity."""
        return list(self.stage_config.keys())

    def get_stage_versions(self, stage: str) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

        :param stage: the stage to get the versions for
        :return: the versions of the queries in the given stage
        """
        return self.stage_config[stage]

    def _insert_into_stage_config(self, stage: str, obj: Union[Transformation, View]):
        if stage not in self.stage_config:
            self.stage_config[stage] = {}
        if obj.name in self.stage_config[stage]:
            raise ConflictingNameError(
                "an object with name {obj.name} already exists in stage {stage}"
            )
        self.stage_config[stage].update({obj.name: obj.version})

        if stage not in self.transformations:
            self.transformations[stage] = {}
        if obj.name in self.transformations[stage]:
            raise ConflictingNameError(
                "an object with name {obj.name} already exists in stage {stage}"
            )
        self.transformations[stage].update({obj.name: obj})

    def add_transformation(self, stage: str, transformation: Transformation):
        """Insert a Transformation into the stage_config of the entity.

        :param stage: the name of the stage where to insert the transformation
        :type stage: str
        :param transformation: the transformation object to insert
        :type transformation: Transformation
        """
        self._insert_into_stage_config(stage, transformation)

    def add_view(self, stage: str, view: View):
        """Insert a View into the stage_config of the entity.

        :param stage: the name of the stage where to insert the transformation
        :type stage: str
        :param view: the View object to insert
        :type view: View
        """
        self._insert_into_stage_config(stage, view)

    def to_dict(self) -> Dict:
        """
        Serialize the entity to a dictionary object.

        :return: the entity as a dictionary object.
        """
        return {
            "name": self.name,
            "version": self.version,
            "kind": self.kind.value if self.kind is not None else None,
            "stage_config": [
                {"name": stage, "versions": self.get_stage_versions(stage)}
                for stage in self.stage_config.keys()
            ]
            if self.stage_config is not None
            else [],
        }

    def __eq__(self, other):
        """Implement the __eq__ method."""
        return (
            self.name == other.name
            and self.version == other.version
            and self.stage_config == other.stage_config
            and self.kind == other.kind
        )


class BaseLayerEntity(Entity):
    """Class that serves as a version configuration for a logical subset of a Pipeline with fixed layers."""

    _stages = ["datalake", "preamble", "staging", "data_warehouse", "data_mart"]

    def __init__(
        self,
        name: str,
        version: str,
        kind: Optional[EntityKind] = None,
        datalake_versions: Dict[str, str] = None,
        preamble_versions: Dict[str, str] = None,
        staging_versions: Dict[str, str] = None,
        data_warehouse_versions: Dict[str, str] = None,
        data_mart_versions: Dict[str, str] = None,
    ):
        """
        Create an BaseLayerEntity object.

        A BaseLayerEntity should be used in the case when the normal layer configuration is being used.
        This means there are 5 layers: datalake, preamble, staging, data_warehouse and data_mart.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        this entity belongs to.
        :param kind: the kind of the entity
        :param datalake_versions: the versions of the queries for the datalake stage
        :param preamble_versions: the versions of the queries for the preamble stage
        :param staging_versions: the versions of the queries for the staging stage
        :param data_warehouse_versions: the versions of the queries for the data warehouse stage
        :param data_mart_versions: the versions of the queries for the data mart stage
        """
        super().__init__(name, version, kind)
        self.datalake_versions = datalake_versions
        self.preamble_versions = preamble_versions
        self.staging_versions = staging_versions
        self.data_warehouse_versions = data_warehouse_versions
        self.data_mart_versions = data_mart_versions
        self.stage_config = self.get_stage_config()

    @classmethod
    def from_dict(cls, d: dict):
        """Create an BaseLayerEntity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: BaseLayerEntity
        :rtype: BaseLayerEntity
        """
        entity = cls(
            name=d["name"],
            version=d["version"],
            kind=EntityKind(d["kind"]) if d.get("kind") is not None else None,
        )
        for stage in d.get("stage_config", {}):
            if stage["name"] == "datalake":
                entity.datalake_versions = stage["versions"]
            elif stage["name"] == "preamble":
                entity.preamble_versions = stage["versions"]
            if stage["name"] == "staging":
                entity.staging_versions = stage["versions"]
            if stage["name"] == "data_warehouse":
                entity.data_warehouse_versions = stage["versions"]
            if stage["name"] == "data_mart":
                entity.data_mart_versions = stage["versions"]
        entity.stage_config = entity.get_stage_config()
        return entity

    @property
    def stages(self):
        """Return a list of all the stages defined in this entity."""
        return self.stages.copy()

    def get_stage_config(self):
        """
        Get the stage config for a base layer entity based on the fixed stages in the BaseLayerEntity.

        :return: a dictionary in the form of a stage config
        """
        return {stage: self.get_stage_versions(stage) for stage in self._stages}

    def get_stage_versions(self, stage: str) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

        :param stage: the stage to get the versions for
        :return: the versions of the queries in the given stage
        """
        if stage == "datalake":
            return self.get_datalake_versions()
        elif stage == "preamble":
            return self.get_preamble_versions()
        if stage == "staging":
            return self.get_staging_versions()
        if stage == "data_warehouse":
            return self.get_data_warehouse_versions()
        if stage == "data_mart":
            return self.get_data_mart_versions()

    def get_datalake_versions(self) -> Dict[str, str]:
        """
        Get the versions of the queries in the datalake stage.

        :return: the versions of the queries in the datalake stage
        """
        if self.datalake_versions is None:
            return {}
        else:
            return self.datalake_versions

    def get_preamble_versions(self) -> Dict[str, str]:
        """
        Get the versions of the queries in the preamble stage.

        :return: the versions of the queries in the preamble stage
        """
        if self.preamble_versions is None:
            return {}
        else:
            return self.preamble_versions

    def get_staging_versions(self) -> Dict[str, str]:
        """
        Get the versions of the queries in the staging stage.

        :return: the versions of the queries in the staging stage
        """
        if self.staging_versions is None:
            return {}
        else:
            return self.staging_versions

    def get_data_warehouse_versions(self) -> Dict[str, str]:
        """
        Get the versions of the queries in the data warehouse stage.

        :return: the versions of the queries in the data warehouse stage
        """
        if self.data_warehouse_versions is None:
            return {}
        else:
            return self.data_warehouse_versions

    def get_data_mart_versions(self) -> Dict[str, str]:
        """
        Get the versions of the queries in the data mart stage.

        :return: the versions of the queries in the data mart stage
        """
        if self.data_mart_versions is None:
            return {}
        else:
            return self.data_mart_versions


class ParametrizedEntity:
    """Class that serves as a version configuration for a logical subset of a ParametrizedPipeline."""

    def __init__(
        self,
        name: str,
        version: str,
        kind: Optional[EntityKind] = None,
        stage_config: Optional[Dict[str, Dict[str, str]]] = None,
        custom_operators: Optional[Dict[str, List[CustomCode]]] = None,
    ):
        """
        Create a ParametrizedEntity object.

        A parametrized entity should be combined with a parametrized pipeline. This allows developers to make behavior
        of the entity dynamic based on the parameters from the pipeline.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        this entity belongs to.
        :param kind: the kind of the entity
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        """
        self.name = name
        self.version = version
        self.kind = kind
        self.stage_config = stage_config or {}
        self.transformations = {}
        self.custom_operators = custom_operators or {}

    @classmethod
    def from_dict(cls, d: dict):
        """Create an ParametrizedEntity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: ParametrizedEntity
        :rtype: ParametrizedEntity
        """
        stage_config = {stage["name"]: stage["versions"] for stage in d["stage_config"]}
        return cls(
            name=d["name"],
            version=d["version"],
            kind=EntityKind(d["kind"]) if d.get("kind") is not None else None,
            stage_config=stage_config,
        )

    @property
    def stages(self):
        """Return a list of all the stages defined in this entity."""
        return list(self.stage_config.keys())

    def get_stage_versions(
        self, stage: str, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

        :param stage: the stage to get the versions for
        :param parameters: the pipeline parameters
        :return: the versions of the queries in the given stage
        """
        return self.stage_config[stage]

    def to_dict(self, parameters: Dict[str, str] = None) -> Dict:
        """
        Serialize the entity to a dictionary object.

        :param parameters: the pipeline parameters
        :return: the entity as a dictionary object.
        """
        return {
            "name": _parametrized_name(self.name, parameters),
            "version": self.version,
            "kind": self.kind.value if self.kind is not None else None,
            "stage_config": [
                {"name": stage, "versions": self.get_stage_versions(stage, parameters)}
                for stage in self.stage_config.keys()
            ],
        }

    def __eq__(self, other):
        """Implement the __eq__ method."""
        return (
            self.name == other.name
            and self.version == other.version
            and self.stage_config == other.stage_config
            and self.kind == other.kind
        )


class ParametrizedBaseLayerEntity(ParametrizedEntity):
    """Class that serves as a version configuration for a logical subset of a ParametrizedPipeline with fixed layers."""

    _stages = ["datalake", "preamble", "staging", "data_warehouse", "data_mart"]

    def __init__(
        self,
        name: str,
        version: str,
        kind: Optional[EntityKind] = None,
        datalake_versions: Dict[str, str] = None,
        preamble_versions: Dict[str, str] = None,
        staging_versions: Dict[str, str] = None,
        data_warehouse_versions: Dict[str, str] = None,
        data_mart_versions: Dict[str, str] = None,
    ):
        """
        Create an BaseLayerEntity object.

        A BaseLayerEntity should be used in the case when the normal layer configuration is being used.
        This means there are 5 layers: datalake, preamble, staging, data_warehouse and data_mart.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        this entity belongs to.
        :param kind: the kind of the entity
        :param datalake_versions: the versions of the queries for the datalake stage
        :param preamble_versions: the versions of the queries for the preamble stage
        :param staging_versions: the versions of the queries for the staging stage
        :param data_warehouse_versions: the versions of the queries for the data warehouse stage
        :param data_mart_versions: the versions of the queries for the data mart stage
        """
        super().__init__(name, version, kind)
        self.datalake_versions = datalake_versions
        self.preamble_versions = preamble_versions
        self.staging_versions = staging_versions
        self.data_warehouse_versions = data_warehouse_versions
        self.data_mart_versions = data_mart_versions
        self.stage_config = self.get_stage_config()

    @classmethod
    def from_dict(cls, d: dict):
        """Create an ParametrizedBaseLayerEntity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: ParametrizedBaseLayerEntity
        :rtype: ParametrizedBaseLayerEntity
        """
        entity = cls(
            name=d["name"],
            version=d["version"],
            kind=EntityKind(d["kind"]) if d.get("kind") is not None else None,
            datalake_versions=d["stage_config"],
            preamble_versions=d["preamble_versions"],
            staging_versions=d["staging_versions"],
            data_warehouse_versions=d["data_warehouse_versions"],
            data_mart_versions=d["data_mart_versions"],
        )
        entity.stage_config = entity.get_stage_config()
        return entity

    @property
    def stages(self):
        """Return a list of all the stages defined in this entity."""
        return self.stages.copy()

    def get_stage_config(self, parameters: Dict[str, str] = None):
        """
        Get the stage config for a base layer entity based on the fixed stages in the BaseLayerEntity.

        :param parameters: the pipeline parameters to get the config for
        :return: a dictionary in the form of a stage config
        """
        return {stage: self.get_stage_versions(stage) for stage in self._stages}

    def get_stage_versions(
        self, stage: str, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

        :param stage: the stage to get the versions for
        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the given stage
        """
        if stage == "datalake":
            return self.get_datalake_versions(parameters)
        elif stage == "preamble":
            return self.get_preamble_versions(parameters)
        if stage == "staging":
            return self.get_staging_versions(parameters)
        if stage == "data_warehouse":
            return self.get_data_warehouse_versions(parameters)
        if stage == "data_mart":
            return self.get_data_mart_versions(parameters)

    def get_datalake_versions(
        self, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the datalake stage.

        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the datalake stage
        """
        if self.datalake_versions is None:
            return {}
        else:
            return self.datalake_versions

    def get_preamble_versions(
        self, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the preamble stage.

        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the preamble stage
        """
        if self.preamble_versions is None:
            return {}
        else:
            return self.preamble_versions

    def get_staging_versions(self, parameters: Dict[str, str] = None) -> Dict[str, str]:
        """
        Get the versions of the queries in the staging stage.

        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the staging stage
        """
        if self.staging_versions is None:
            return {}
        else:
            return self.staging_versions

    def get_data_warehouse_versions(
        self, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the data warehouse stage.

        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the data warehouse stage
        """
        if self.data_warehouse_versions is None:
            return {}
        else:
            return self.data_warehouse_versions

    def get_data_mart_versions(
        self, parameters: Dict[str, str] = None
    ) -> Dict[str, str]:
        """
        Get the versions of the queries in the data mart stage.

        :param parameters: the pipeline parameters to get the versions for
        :return: the versions of the queries in the data mart stage
        """
        if self.data_mart_versions is None:
            return {}
        else:
            return self.data_mart_versions


def _parametrized_name(name: str, parameters: Dict[str, str]) -> str:
    """Generate a unique entity name that includes the parameters value.

    :param name: original name
    :type name: str
    :param parameters: parameters applied to apply
    :type parameters: dict
    :return: parametrized parametrized name
    :rtype: str
    """
    if not parameters:
        return name

    # must follow https://cloud.google.com/bigquery/docs/datasets#dataset-naming
    parts = [name, *parameters.values()]
    new_name = "_".join(parts)

    if len(new_name) > 1024:
        raise ValueError(
            f"the size of the name ({new_name}) is to big, maximum size is 1024 characters"
        )
    return new_name
