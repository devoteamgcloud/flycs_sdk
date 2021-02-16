"""This module contains different type of Pipeline triggers."""


from abc import ABC, abstractclassmethod


class PipelineTrigger(ABC):
    """Base class for all pipeline trigger."""

    @abstractclassmethod
    def from_dict(self, d: dict):
        """Create a PipelineTrigger object form a dictionnary created with the to_dict method."""
        pass


class PubSubTrigger(PipelineTrigger):
    """Class used to define a pipeline trigger using a PubSub topic."""

    _kind = "pubsub"

    def __init__(self, topic: str, subscription_project: str = None):
        """Create a new PubSubTrigger object.

        :param topic: pubsub topic
        :type topic: str
        :param subscription_project: The project where to create the subscription, if not specified, the ops project will be used
        :type subscription_project: str
        """
        self.topic = topic
        self.subscription_project = subscription_project

    @classmethod
    def from_dict(cls, d: dict):
        """Create a PubSubTrigger object form a dictionnary created with the to_dict method."""
        return cls(topic=d["topic"], subscription_project=d.get("subscription_project"))

    def to_dict(self):
        """
        Serialize the PubSubTrigger to a dictionary object.

        :return: the PubSubTrigger as a dictionary object.
        :rtype: Dict
        """
        return {
            "type": self._kind,
            "topic": self.topic,
            "subscription_project": self.subscription_project,
        }

    def __eq__(self, other) -> bool:
        """Implement __eq__ method."""
        return (
            self.topic == other.topic
            and self.subscription_project == other.subscription_project
        )


class GCSPrefixWatchTrigger(PipelineTrigger):
    """Class used to define a pipeline trigger by watching a prefix on Google Cloud Storage."""

    _kind = "gcs_watch_prefix"

    def __init__(self, bucket: str, prefix: str = None):
        """Create a new GCSPrefixWatchTrigger object.

        :param bucket: bucket name
        :type bucket: str
        :param prefix: prefix to watch in the bucket, if not specified, the full bucket is watched
        :type prefix: str
        """
        self.bucket = bucket
        self.prefix = prefix

    @classmethod
    def from_dict(cls, d: dict):
        """Create a GCSPrefixWatchTrigger object form a dictionnary created with the to_dict method."""
        return cls(bucket=d["bucket"], prefix=d.get("prefix"))

    def to_dict(self):
        """
        Serialize the GCSPrefixWatchTrigger to a dictionary object.

        :return: the GCSPrefixWatchTrigger as a dictionary object.
        :rtype: Dict
        """
        return {
            "type": self._kind,
            "bucket": self.bucket,
            "prefix": self.prefix,
        }

    def __eq__(self, other) -> bool:
        """Implement __eq__ method."""
        return self.bucket == other.bucket and self.prefix == other.prefix


class GCSObjectExistTrigger(PipelineTrigger):
    """Class used to define a pipeline trigger by watching if an object exists on Google Cloud Storage."""

    _kind = "gcs_object_exist"

    def __init__(self, bucket: str, object: str = None):
        """Create a new GCSTrigger object.

        :param bucket: bucket name
        :type bucket: str
        :param object: object name, if specified, the trigger will watch for existence of this object
        :type object: str
        """
        self.bucket = bucket
        self.object = object

    @classmethod
    def from_dict(cls, d: dict):
        """Create a GCSObjectExistTrigger object form a dictionnary created with the to_dict method."""
        return cls(bucket=d["bucket"], object=d.get("object"))

    def to_dict(self):
        """
        Serialize the GCSObjectExistTrigger to a dictionary object.

        :return: the GCSObjectExistTrigger as a dictionary object.
        :rtype: Dict
        """
        return {
            "type": self._kind,
            "bucket": self.bucket,
            "object": self.object,
        }

    def __eq__(self, other) -> bool:
        """Implement __eq__ method."""
        return self.bucket == other.bucket and self.object == other.object


class GCSObjectChangeTrigger(PipelineTrigger):
    """Class used to define a pipeline trigger by watching if an object changes on Google Cloud Storage."""

    _kind = "gcs_object_change"

    def __init__(self, bucket: str, prefix: str = None, object: str = None):
        """Create a new GCSObjectChangeTrigger object.

        :param bucket: bucket name
        :type bucket: str
        :param object: object name, if specified, the trigger will watch for existence of this object
        :type object: str
        """
        self.bucket = bucket
        self.object = object

    @classmethod
    def from_dict(cls, d: dict):
        """Create a GCSObjectChangeTrigger object form a dictionnary created with the to_dict method."""
        return cls(bucket=d["bucket"], prefix=d.get("prefix"), object=d.get("object"))

    def to_dict(self):
        """
        Serialize the GCSObjectChangeTrigger to a dictionary object.

        :return: the GCSObjectChangeTrigger as a dictionary object.
        :rtype: Dict
        """
        return {
            "type": self._kind,
            "bucket": self.bucket,
            "object": self.object,
        }

    def __eq__(self, other) -> bool:
        """Implement __eq__ method."""
        return self.bucket == other.bucket and self.object == other.object


_triggers = [
    PubSubTrigger,
    GCSPrefixWatchTrigger,
    GCSObjectExistTrigger,
    GCSObjectChangeTrigger,
]


def trigger_factory(typ: str) -> PipelineTrigger:
    """Return the correct trigger type based on its kind."""
    for trigger in _triggers:
        if typ == trigger._kind:
            return trigger
    raise TypeError(f"unsupported trigger type: {typ}")
