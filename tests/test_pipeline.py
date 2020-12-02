#!/usr/bin/env python
"""Tests for `flycs_sdk` package."""
# pylint: disable=redefined-outer-name

import pytest
from deepdiff import DeepDiff
from flycs_sdk.entities import Entity
from flycs_sdk.pipelines import Pipeline, PipelineKind

pipeline_name = "test"
pipeline_version = "1.0.0"
pipeline_schedule = "* 12 * * *"
pipeline_kind = PipelineKind.VANILLA
pipeline_start_time = 1606923514


class TestEntity:
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return Entity("test", "1.0.0", stage_config)

    @pytest.fixture
    def my_pipeline(self, my_entity):
        return Pipeline(
            name=pipeline_name,
            version=pipeline_version,
            schedule=pipeline_schedule,
            kind=pipeline_kind,
            start_time=pipeline_start_time,
            entities=[],
        )

    def test_init(self, my_pipeline):
        assert my_pipeline.name == pipeline_name
        assert my_pipeline.version == pipeline_version
        assert my_pipeline.schedule == pipeline_schedule
        assert my_pipeline.start_time == pipeline_start_time
        assert my_pipeline.kind == pipeline_kind
        assert my_pipeline.entities == []

    def test_add_entity(self, my_pipeline, my_entity):
        assert my_pipeline.entities == []
        my_pipeline.add_entity(my_entity)
        assert my_pipeline.entities == [my_entity]

    def test_to_dict(self, my_pipeline, my_entity):
        my_pipeline.add_entity(my_entity)
        actual = my_pipeline.to_dict()
        expected = {
            "name": pipeline_name,
            "version": pipeline_version,
            "schedule": pipeline_schedule,
            "kind": pipeline_kind.value,
            "start_time": pipeline_start_time,
            "entities": [my_entity.to_dict()],
        }
        assert not DeepDiff(actual, expected, ignore_order=True,)
