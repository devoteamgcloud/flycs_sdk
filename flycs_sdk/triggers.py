"""This module contains different type of Pipeline triggers."""


from abc import ABC


class PipelineTrigger(ABC):
    """Base class for all pipeline trigger."""

    pass


class PubSubTrigger(PipelineTrigger):
    """Class used to define a pipeline trigger using a PubSub topic."""

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
            "type": "pubsub",
            "topic": self.topic,
            "subscription_project": self.subscription_project,
        }

    def __eq__(self, other) -> bool:
        """Implement __eq__ method."""
        return (
            self.topic == other.topic
            and self.subscription_project == other.subscription_project
        )
