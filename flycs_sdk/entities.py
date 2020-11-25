"""Module containing entity classes."""

from typing import Dict


class Entity:
    """Class that is used for converting an entity class to an Airflow DAG."""

    def __init__(
        self,
        name: str,
        version: str,
        parameters: Dict[str, str] = None,
        stage_config: Dict[str, Dict[str, str]] = None,
    ):
        """
        Create an Entity object.

        :param name: the name of the entity
        :param version: the version of the entity, this can be used for table naming
        :param parameters: the parameters as set in the running environment. These are inherited from the pipeline
        this entity belongs to.
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        """
        self.name = name
        self.version = version
        self.parameters = parameters
        self.stage_config = stage_config

    def get_stage_versions(self, stage: str) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

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
            ],
        }


class BaseLayerEntity(Entity):
    """Class that is used for converting an entity class to an Airflow DAG."""

    stages = ["datalake", "preamble", "staging", "data_warehouse", "data_mart"]

    def __init__(
        self,
        name: str,
        version: str,
        parameters: Dict[str, str] = None,
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
        :param parameters: the parameters as set in the running environment. These are inherited from the pipeline
        this entity belongs to.
        :param stage_config: a dictionary with the name of the stage as key and a dictionary of query names
        and their versions as value.
        """
        super().__init__(name, version, parameters)
        self.datalake_versions = datalake_versions
        self.preamble_versions = preamble_versions
        self.staging_versions = staging_versions
        self.data_warehouse_versions = data_warehouse_versions
        self.data_mart_versions = data_mart_versions
        self.stage_config = self.get_stage_config()

    def get_stage_config(self):
        """
        Get the stage config for a base layer entity based on the fixed stages in the BaseLayerEntity.

        :return: a dictionary in the form of a stage config
        """
        return {stage: self.get_stage_versions(stage) for stage in self.stages}

    def get_stage_versions(self, stage: str) -> Dict[str, str]:
        """
        Get the versions of the queries in the given stage.

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
