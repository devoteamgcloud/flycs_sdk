"""Module containing entity classes."""

from typing import Dict


class Entity:
    """Class that serves as a version configuration for a logical subset of a Pipeline."""

    def __init__(
        self, name: str, version: str, stage_config: Dict[str, Dict[str, str]] = None,
    ):
        """
        Create an Entity object.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        this entity belongs to.
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        """
        self.name = name
        self.version = version
        self.stage_config = stage_config
        self.transformations = {}

    @classmethod
    def from_dict(cls, d: dict):
        """Create an Entity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: Entity
        :rtype: Entity
        """
        stage_config = {stage["name"]: stage["versions"] for stage in d["stage_config"]}
        return cls(name=d["name"], version=d["version"], stage_config=stage_config)

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

    def to_dict(self) -> Dict:
        """
        Serialize the entity to a dictionary object.

        :return: the entity as a dictionary object.
        """
        return {
            "name": self.name,
            "version": self.version,
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
        )


class BaseLayerEntity(Entity):
    """Class that serves as a version configuration for a logical subset of a Pipeline with fixed layers."""

    _stages = ["datalake", "preamble", "staging", "data_warehouse", "data_mart"]

    def __init__(
        self,
        name: str,
        version: str,
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
        :param datalake_versions: the versions of the queries for the datalake stage
        :param preamble_versions: the versions of the queries for the preamble stage
        :param staging_versions: the versions of the queries for the staging stage
        :param data_warehouse_versions: the versions of the queries for the data warehouse stage
        :param data_mart_versions: the versions of the queries for the data mart stage
        """
        super().__init__(name, version)
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
        entity = cls(name=d["name"], version=d["version"])
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
        self, name: str, version: str, stage_config: Dict[str, Dict[str, str]] = None
    ):
        """
        Create a ParametrizedEntity object.

        A parametrized entity should be combined with a parametrized pipeline. This allows developers to make behavior
        of the entity dynamic based on the parameters from the pipeline.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        this entity belongs to.
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        """
        self.name = name
        self.version = version
        self.stage_config = stage_config
        self.queries = {}

    @classmethod
    def from_dict(cls, d: dict):
        """Create an ParametrizedEntity object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: ParametrizedEntity
        :rtype: ParametrizedEntity
        """
        stage_config = {stage["name"]: stage["versions"] for stage in d["stage_config"]}
        return cls(name=d["name"], version=d["version"], stage_config=stage_config)

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
        )


class ParametrizedBaseLayerEntity(ParametrizedEntity):
    """Class that serves as a version configuration for a logical subset of a ParametrizedPipeline with fixed layers."""

    _stages = ["datalake", "preamble", "staging", "data_warehouse", "data_mart"]

    def __init__(
        self,
        name: str,
        version: str,
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
        :param datalake_versions: the versions of the queries for the datalake stage
        :param preamble_versions: the versions of the queries for the preamble stage
        :param staging_versions: the versions of the queries for the staging stage
        :param data_warehouse_versions: the versions of the queries for the data warehouse stage
        :param data_mart_versions: the versions of the queries for the data mart stage
        """
        super().__init__(name, version)
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
