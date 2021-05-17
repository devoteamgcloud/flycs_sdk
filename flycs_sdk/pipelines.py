"""Module containing pipeline classes."""

import itertools
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Union, Tuple, Optional

from semver import VersionInfo

from .entities import (
    BaseLayerEntity,
    Entity,
    ParametrizedBaseLayerEntity,
    ParametrizedEntity,
    _parametrized_name,
)

from .triggers import PipelineTrigger, trigger_factory


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
        entities: List[
            Union[
                Entity, BaseLayerEntity, ParametrizedEntity, ParametrizedBaseLayerEntity
            ]
        ] = None,
        schedule: Optional[str] = None,
        kind: PipelineKind = PipelineKind.VANILLA,
        start_time: Optional[datetime] = None,
        trigger: Optional[PipelineTrigger] = None,
        params: Optional[Dict[str, str]] = None,
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
        :param trigger: special pipeline trigger. If specified, the pipeline can will be automatically triggered by different events like a PubSub message or a Google Storage event
        :type trigger: PipelineTrigger, optional
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
        self.trigger = trigger if _is_valid_trigger(trigger) else None
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
        obj = cls(
            name=d["name"],
            version=d["version"],
            schedule=d.get("schedule"),
            start_time=_parse_datetime(d["start_time"])
            if d.get("start_time")
            else None,
            kind=PipelineKind(d["kind"]),
            params=d.get("params", {}),
            entities=[Entity.from_dict(e) for e in d["entities"]],
        )

        if d.get("trigger"):
            trigger_class = trigger_factory(d["trigger"].get("type"))
            obj.trigger = trigger_class.from_dict(d["trigger"])

        return obj

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
        schedule = self.schedule
        if isinstance(self.schedule, Pipeline):
            schedule = format_target_pipeline(self.schedule)

        return {
            "name": self.name,
            "version": self.version,
            "schedule": schedule,
            "start_time": _format_datetime(self.start_time)
            if self.start_time
            else None,
            "trigger": self.trigger.to_dict() if self.trigger else None,
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
            and self.trigger == other.trigger
            and self.entities == other.entities
        )


class ParametrizedPipeline:
    """Class ParametrizedPipeline represents a dynamic pipeline configuration."""

    def __init__(
        self,
        name: str,
        version: str,
        entities: List[Union[ParametrizedEntity, ParametrizedBaseLayerEntity]] = None,
        schedule: Optional[str] = None,
        kind: PipelineKind = PipelineKind.VANILLA,
        start_time: Optional[datetime] = None,
        trigger: Optional[PipelineTrigger] = None,
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
        :param trigger: special pipeline trigger. If specified, the pipeline can will be automatically triggered by different events like a PubSub message or a Google Storage event
        :type trigger: PipelineTrigger, optional
        :param parameters: pipeline parameters that will be passed to each entities contained in the pipeline during rendering
        :type parameters: dict, optional
        """
        self.name = name
        if _is_valid_version(version):
            self.version = version
        self._schedule = schedule  # TODO: validate format
        self.kind = kind
        if _is_valid_start_time(start_time):
            self._start_time = start_time or datetime.now()
        self.trigger = trigger if _is_valid_trigger(trigger) else None
        self.entities = entities or []
        self.parameters = parameters

    @property
    def schedule(self):
        """Property used by sub-class to modify the schedule value based on the parameters of the pipeline."""
        return self._schedule

    @schedule.setter
    def schedule(self, value: str):
        self._schedule = None

    @property
    def start_time(self):
        """Property used by sub-class to modify the start_time value based on the parameters of the pipeline."""
        return self._start_time

    @start_time.setter
    def start_time(self, value: datetime):
        """Set start_time."""
        if value is not None:
            _is_valid_start_time(value)
        self._start_time = value

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

    def unrolled_pipelines(self) -> List[Pipeline]:
        """Return a list of Pipeline object, one for each parameters combination.

        :return: List of Pipeline object
        :rtype: List[Pipeline]
        """
        parameters = [
            dict(zip(self.parameters.keys(), x))
            for x in itertools.product(*self.parameters.values())
        ]

        return [
            Pipeline(
                name=_parametrized_name(self.name, p),
                version=self.version,
                schedule=self.schedule,
                entities=self.entities,
                kind=self.kind,
                start_time=self.start_time,
                params=p,
            )
            for p in parameters
        ]

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

        schedule = self.schedule
        if isinstance(self.schedule, ParametrizedPipeline):
            schedule = format_target_pipeline(self.schedule)

        return [
            {
                "name": _parametrized_name(self.name, p),
                "version": self.version,
                "schedule": schedule,
                "start_time": _format_datetime(self.start_time)
                if self.start_time
                else None,
                "trigger": self.trigger.to_dict() if self.trigger else None,
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


def _is_valid_start_time(start_time: Optional[datetime]) -> bool:
    """Test if start_time is a valid timestamp value.

    :param start_time: timestamp to validate
    :type start_time: int
    :raises ValueError
    :return: True is start_time is valid
    :rtype: bool
    """
    if start_time is None:
        return True

    if not isinstance(start_time, datetime):
        raise TypeError("start_time must be a valid datetime object")

    if start_time.tzinfo != timezone.utc:
        raise ValueError("start_time timezone must be UTC")

    return True


def _is_valid_trigger(trigger: PipelineTrigger) -> bool:
    if trigger is not None and not isinstance(trigger, PipelineTrigger):
        raise TypeError("trigger must be a valid PipelineTrigger subclass")
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


def format_target_pipeline(p: Pipeline) -> str:
    """Format the name of a pipeline to be used in the schedule field."""
    return f"{p.name}_{p.version}"


def parse_target_pipeline(target: str) -> Tuple:
    """Parse a pipeline name generated from format_target_pipeline and return both name and version."""
    if not target:
        raise ValueError(f"pipeline target name is not valid: {target}")
    ss = target.split("_")
    if len(ss) < 2:
        raise ValueError(f"pipeline target name is not valid: {target}")
    name = "_".join(ss[: len(ss) - 1])  # support for name containing underscore already
    version = ss[-1]
    return (name, version)
