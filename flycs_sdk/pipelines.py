"""Module containing pipeline classes."""

import itertools
import time
from enum import Enum
from typing import Dict, List, Union

from semver import VersionInfo

from .entities import (
    BaseLayerEntity,
    Entity,
    ParametrizedBaseLayerEntity,
    ParametrizedEntity,
)


class PipelineKind(Enum):
    """This enumeration contains all the supported pipeline type."""

    VANILLA = "vanilla"
    DELTA_TRACKING = "delta_tracking"
    DATA_VAULT = "data_vault"


class Pipeline:
    """Class representing a pipeline configuration."""

    def __init__(
        self,
        name: str,
        version: str,
        schedule: str,
        entities: List[
            Union[
                Entity, BaseLayerEntity, ParametrizedEntity, ParametrizedBaseLayerEntity
            ]
        ] = None,
        kind: PipelineKind = PipelineKind.VANILLA,
        start_time: int = None,
    ):
        """
        Create a Pipeline object.

        :param name: the name of the pipeline
        :type name: str
        :param version: the version of the pipeline
        :type version: str
        :param schedule: the scheduler definition using cron format
        :kind schedule: str
        :param kind: the type of the pipeline. the type determines what actions will be taken aside from just running the queries
        :type type: PipelineKind, default to vanilla
        :param start_time: timestamp at which the pipeline should start to be processed, defaults to None
        :type start_time: int, optional
        """
        self.name = name
        if _is_valid_version(version):
            self.version = version
        self.schedule = schedule  # TODO: validate format
        self.kind = kind
        if _is_valid_start_time(start_time):
            self.start_time = start_time or int(time.time())
        self.entities = entities

    @classmethod
    def from_dict(cls, d: dict):
        """Create a Pipeline object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: Pipeline
        :rtype: Pipeline
        """
        return cls(
            name=d["name"],
            version=d["version"],
            schedule=d["schedule"],
            start_time=d["start_time"],
            kind=PipelineKind(d["kind"]),
            entities=[
                Entity.from_dict(e) for e in d["entities"]
            ],  # TODO: detect type of entity
        )

    def add_entity(
        self,
        entity: Union[
            Entity, BaseLayerEntity, ParametrizedEntity, ParametrizedBaseLayerEntity
        ],
    ):
        """
        Add entity to the list of entities contained in this pipeline.

        :return: None
        """
        return self.entities.append(entity)

    def to_dict(self) -> Dict:
        """
        Serialize the pipeline to a dictionary object.

        :return: the pipeline as a dictionary object.
        :rtype: Dict
        """
        return {
            "name": self.name,
            "version": self.version,
            "schedule": self.schedule,
            "start_time": self.start_time,
            "kind": self.kind.value,
            "entities": [e.to_dict() for e in self.entities],
        }

    def __eq__(self, other):
        """Implement __eq__ method."""
        return (
            self.name == other.name
            and self.version == other.version
            and self.schedule == other.schedule
            and self.kind.value == other.kind.value
            and self.start_time == other.start_time
            and self.entities == other.entities
        )


class ParametrizedPipeline:
    """Class ParametrizedPipeline represents a dynamic pipeline configuration."""

    def __init__(
        self,
        name: str,
        version: str,
        schedule: str,
        entities: List[Union[ParametrizedEntity, ParametrizedBaseLayerEntity]] = None,
        kind: PipelineKind = PipelineKind.VANILLA,
        start_time: int = None,
        parameters: Dict[str, List[str]] = None,
    ):
        """
        Create a ParametrizedPipeline object.

        :param name: the name of the pipeline
        :type name: str
        :param version: the version of the pipeline
        :type version: str
        :param schedule: the scheduler definition using cron format
        :kind schedule: str
        :param kind: the type of the pipeline. the type determines what actions will be taken aside from just running the queries
        :type type: PipelineKind, default to vanilla
        :param start_time: timestamp at which the pipeline should start to be processed, defaults to None
        :type start_time: int, optional
        :param parameters: pipeline parameters that will be passed to each entities contained in the pipeline during rendering
        :type parameters: dict, optional
        """
        self.name = name
        if _is_valid_version(version):
            self.version = version
        self.schedule = schedule  # TODO: validate format
        self.kind = kind
        if _is_valid_start_time(start_time):
            self.start_time = start_time or int(time.time())
        self.entities = entities
        self.parameters = parameters

    def add_entity(
        self, entity: Union[ParametrizedEntity, ParametrizedBaseLayerEntity],
    ):
        """
        Add entity to the list of entities contained in this pipeline.

        :raises: TypeError
        :return: None
        """
        if not isinstance(entity, (ParametrizedEntity, ParametrizedBaseLayerEntity)):
            raise TypeError(
                "entity type not valid, this pipeline only supports parameterized entity"
            )
        return self.entities.append(entity)

    def to_dict(self) -> Dict:
        """
        Serialize the pipeline to a dictionary object.

        :return: the pipeline as a dictionary object.
        :rtype: Dict
        """
        # creates a list of all possible combination of parameter
        # for a self.parameters like: {"language": ["nl", "fr"], "country": ["be", "en"]}
        # it creates a list like:
        # [
        #     {"language": "nl", "country": "be"},
        #     {"language": "nl", "country": "en"},
        #     {"language": "fr", "country": "be"},
        #     {"language": "fr", "country": "en"},
        # ]
        parameters = [
            dict(zip(self.parameters.keys(), x))
            for x in itertools.product(*self.parameters.values())
        ]

        d = {
            "name": self.name,
            "version": self.version,
            "schedule": self.schedule,
            "start_time": self.start_time,
            "kind": self.kind.value,
            "entities": [
                e.to_dict(parameters=p) for p in parameters for e in self.entities
            ],
        }
        return d


def _is_valid_version(version: str) -> bool:
    """Test if version is using a valid semver format.

    :param version: version to validate
    :type version: str
    :raises: ValueError
    :return: true if version has a valid format
    :rtype: bool
    """
    VersionInfo.parse(version)
    return True


def _is_valid_start_time(start_time: int) -> bool:
    """Test if start_time is a valid timestamp value.

    :param start_time: timestamp to validate
    :type start_time: int
    :raises ValueError
    :return: True is start_time is valid
    :rtype: bool
    """
    if start_time < 0:
        raise ValueError("start_time can not be a negative value")
    return True
