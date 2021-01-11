"""Module containing pipeline classes."""

import itertools
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Union

from semver import VersionInfo

from .entities import (
    BaseLayerEntity,
    Entity,
    ParametrizedBaseLayerEntity,
    ParametrizedEntity,
    _parametrized_name,
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
        start_time: datetime = None,
        params: Dict[str, str] = None,
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
        :param start_time: timestamp at which the pipeline should start to be processed. The time MUST always be expressed using UTC timezone, defaults to None
        :type start_time: datetime, optional
        :param params: parameters that can be used as template input data for the queries of this pipelines
        :type params: dict, optional
        """
        self.name = name
        if _is_valid_version(version):
            self.version = version
        self.schedule = schedule  # TODO: validate format
        self.kind = kind
        if _is_valid_start_time(start_time):
            self.start_time = start_time or datetime.now()
        self.entities = entities or []
        self.params = params or {}

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
            start_time=_parse_datetime(d["start_time"]),
            kind=PipelineKind(d["kind"]),
            params=d.get("params", {}),
            entities=[Entity.from_dict(e) for e in d["entities"]],
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
            "start_time": _format_datetime(self.start_time),
            "kind": self.kind.value,
            "params": self.params,
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
        start_time: datetime = None,
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
        :param start_time: timestamp at which the pipeline should start to be processed. The time MUST always be expressed using UTC timezone, defaults to None
        :type start_time: datetime, optional
        :param parameters: pipeline parameters that will be passed to each entities contained in the pipeline during rendering
        :type parameters: dict, optional
        """
        self.name = name
        if _is_valid_version(version):
            self.version = version
        self._schedule = schedule  # TODO: validate format
        self.kind = kind
        self._start_time = start_time or datetime.now()
        self.entities = entities
        self.parameters = parameters

    @property
    def schedule(self):
        """Property used by sub-class to modify the schedule value based on the parameters of the pipeline."""
        return self._schedule

    @property
    def start_time(self):
        """Property used by sub-class to modify the start_time value based on the parameters of the pipeline."""
        return self._start_time

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

    def to_dict(self) -> List[Dict]:
        """
        Serialize the pipeline to a list of dictionary object.

        for each possible combination of the parameters a new item in the list is created

        :return: the list of parametrized pipeline.
        :rtype: List
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

        return [
            {
                "name": _parametrized_name(self.name, p),
                "version": self.version,
                "schedule": self.schedule,
                "start_time": _format_datetime(self.start_time),
                "kind": self.kind.value,
                "params": p,
                "entities": [e.to_dict(parameters=p) for e in self.entities],
            }
            for p in parameters
        ]


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


def _is_valid_start_time(start_time: datetime) -> bool:
    """Test if start_time is a valid timestamp value.

    :param start_time: timestamp to validate
    :type start_time: int
    :raises ValueError
    :return: True is start_time is valid
    :rtype: bool
    """
    if not isinstance(start_time, datetime):
        raise TypeError("start_time must be a a valid datetime object")

    if start_time.tzinfo != timezone.utc:
        raise ValueError("start_time timezone must be UTC")

    return True


# sine we support python3.6 we cannot use datetime fromisoformat and isoformat methods
# instead we use this
_time_format = "%Y-%m-%dT%H:%M:%S%z"


def _format_datetime(t: datetime) -> str:
    return t.strftime(_time_format)


def _parse_datetime(tstr: str) -> datetime:

    # ensure we always have the UTC timezone information
    if not tstr.endswith("+0000"):
        tstr += "+0000"
    return datetime.strptime(tstr, _time_format)
