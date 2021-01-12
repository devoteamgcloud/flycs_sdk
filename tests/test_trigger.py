from flycs_sdk.triggers import PubSubTrigger

import pytest

pubsub_topic = "projects/ops-dta-dummy-fl1/topics/test-trigger-pipeline"
pubsub_subscription_project = "ops-dta-dummy-fl1"


class TestTriggers:
    @pytest.fixture
    def my_pubsub(self) -> PubSubTrigger:
        return PubSubTrigger(
            topic=pubsub_topic, subscription_project=pubsub_subscription_project
        )

    def test_init(self, my_pubsub: PubSubTrigger):
        assert my_pubsub.topic == pubsub_topic
        assert my_pubsub.subscription_project == pubsub_subscription_project

    def test_to_dict(self, my_pubsub):
        assert my_pubsub.to_dict() == {
            "type": "pubsub",
            "topic": "projects/ops-dta-dummy-fl1/topics/test-trigger-pipeline",
            "subscription_project": "ops-dta-dummy-fl1",
        }

    def test_from_dict(self, my_pubsub):
        loaded = PubSubTrigger.from_dict(my_pubsub.to_dict())
        assert loaded == my_pubsub
