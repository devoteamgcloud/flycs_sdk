from flycs_sdk.triggers import (
    PubSubTrigger,
    GCSObjectChangeTrigger,
    GCSObjectExistTrigger,
    GCSPrefixWatchTrigger,
)

import pytest

pubsub_topic = "projects/ops-dta-dummy-fl1/topics/test-trigger-pipeline"
pubsub_subscription_project = "ops-dta-dummy-fl1"


class TestPubSubTriggers:
    @pytest.fixture
    def my_trigger(self) -> PubSubTrigger:
        return PubSubTrigger(
            topic=pubsub_topic, subscription_project=pubsub_subscription_project
        )

    def test_init(self, my_trigger: PubSubTrigger):
        assert my_trigger.topic == pubsub_topic
        assert my_trigger.subscription_project == pubsub_subscription_project

    def test_to_dict(self, my_trigger):
        assert my_trigger.to_dict() == {
            "type": "pubsub",
            "topic": "projects/ops-dta-dummy-fl1/topics/test-trigger-pipeline",
            "subscription_project": "ops-dta-dummy-fl1",
        }

    def test_from_dict(self, my_trigger):
        loaded = PubSubTrigger.from_dict(my_trigger.to_dict())
        assert loaded == my_trigger


gcs_bucket = "bucket"
gcs_prefix = "prefix"
gcs_object = "object"


class TestGCSTriggers:
    @pytest.fixture
    def prefix_watch(self) -> GCSPrefixWatchTrigger:
        return GCSPrefixWatchTrigger(bucket=gcs_bucket, prefix=gcs_prefix)

    @pytest.fixture
    def object_exist(self) -> GCSObjectExistTrigger:
        return GCSObjectExistTrigger(bucket=gcs_bucket, object=gcs_object)

    @pytest.fixture
    def object_change(self) -> GCSObjectChangeTrigger:
        return GCSObjectChangeTrigger(bucket=gcs_bucket, object=gcs_object)

    def test_init_prefix(self, prefix_watch):
        assert prefix_watch.bucket == gcs_bucket
        assert prefix_watch.prefix == gcs_prefix

    def test_init_exists(self, object_exist):
        assert object_exist.bucket == gcs_bucket
        assert object_exist.object == gcs_object

    def test_init_change(self, object_change):
        assert object_change.bucket == gcs_bucket
        assert object_change.object == gcs_object

    def test_to_dict_prefix(self, prefix_watch):
        assert prefix_watch.to_dict() == {
            "type": "gcs_watch_prefix",
            "bucket": "bucket",
            "prefix": "prefix",
        }

    def test_to_dict_exist(self, object_exist):
        assert object_exist.to_dict() == {
            "type": "gcs_object_exist",
            "bucket": "bucket",
            "object": "object",
        }

    def test_to_dict_update(self, object_change):
        assert object_change.to_dict() == {
            "type": "gcs_object_change",
            "bucket": "bucket",
            "object": "object",
        }

    def test_from_dict_prefix(self, prefix_watch):
        loaded = GCSPrefixWatchTrigger.from_dict(prefix_watch.to_dict())
        assert loaded == prefix_watch

    def test_from_dict_prefix(self, object_exist):
        loaded = GCSObjectExistTrigger.from_dict(object_exist.to_dict())
        assert loaded == object_exist

    def test_from_dict_prefix(self, object_change):
        loaded = GCSObjectChangeTrigger.from_dict(object_change.to_dict())
        assert loaded == object_change
