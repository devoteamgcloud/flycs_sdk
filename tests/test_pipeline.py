#!/usr/bin/env python
"""Tests for `flycs_sdk` package."""
# pylint: disable=redefined-outer-name

import pytest
from deepdiff import DeepDiff
from flycs_sdk.entities import Entity, ParametrizedEntity
from flycs_sdk.pipelines import Pipeline, ParametrizedPipeline, PipelineKind

pipeline_name = "test"
pipeline_version = "1.0.0"
pipeline_schedule = "* 12 * * *"
pipeline_kind = PipelineKind.VANILLA
pipeline_start_time = 1606923514


class TestPipeline:
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

    def test_invalid_start_time(self):
        with pytest.raises(ValueError):
            return Pipeline(
                name=pipeline_name,
                version=pipeline_version,
                schedule=pipeline_schedule,
                kind=pipeline_kind,
                start_time=-1,
                entities=[],
            )

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
            "entities": [
                {
                    "name": "test",
                    "version": "1.0.0",
                    "stage_config": [
                        {
                            "name": "raw",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                        {
                            "name": "staging",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                    ],
                }
            ],
        }
        assert expected == actual

    def test_serialize_deserialize(self, my_pipeline, my_entity):
        my_pipeline.add_entity(my_entity)
        d = my_pipeline.to_dict()
        loaded = Pipeline.from_dict(d)
        loaded.entities = []
        my_pipeline.entities = []
        assert loaded == my_pipeline


pipeline_parameters = {"language": ["nl", "fr"], "country": ["be", "en"]}


class TestParametrizedPipeline(TestPipeline):
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return ParametrizedEntity("test", "1.0.0", stage_config)

    @pytest.fixture
    def my_non_parameterized_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return Entity("test", "1.0.0", stage_config)

    @pytest.fixture
    def my_pipeline(self, my_entity):
        return ParametrizedPipeline(
            name=pipeline_name,
            version=pipeline_version,
            schedule=pipeline_schedule,
            kind=pipeline_kind,
            start_time=pipeline_start_time,
            entities=[],
            parameters=pipeline_parameters,
        )

    def test_add_non_parametrized_entity(
        self, my_pipeline, my_non_parameterized_entity
    ):
        with pytest.raises(
            TypeError,
            match="entity type not valid, this pipeline only supports parameterized entity",
        ):
            my_pipeline.add_entity(my_non_parameterized_entity)

    def test_to_dict(self, my_pipeline, my_entity):
        my_pipeline.add_entity(my_entity)
        actual = my_pipeline.to_dict()
        expected = {
            "name": "test",
            "version": "1.0.0",
            "schedule": "* 12 * * *",
            "start_time": 1606923514,
            "kind": "vanilla",
            "entities": [
                {
                    "name": "test_nl_be",
                    "version": "1.0.0",
                    "stage_config": [
                        {
                            "name": "raw",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                        {
                            "name": "staging",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                    ],
                },
                {
                    "name": "test_nl_en",
                    "version": "1.0.0",
                    "stage_config": [
                        {
                            "name": "raw",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                        {
                            "name": "staging",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                    ],
                },
                {
                    "name": "test_fr_be",
                    "version": "1.0.0",
                    "stage_config": [
                        {
                            "name": "raw",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                        {
                            "name": "staging",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                    ],
                },
                {
                    "name": "test_fr_en",
                    "version": "1.0.0",
                    "stage_config": [
                        {
                            "name": "raw",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                        {
                            "name": "staging",
                            "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                        },
                    ],
                },
            ],
        }
        assert expected == actual

    def test_entity_name_container_parameters(self, my_pipeline, my_entity):
        my_pipeline.add_entity(my_entity)
        d = my_pipeline.to_dict()
        assert len(d["entities"]) == 4
        assert sorted(
            ["test_nl_be", "test_nl_en", "test_fr_be", "test_fr_en"]
        ) == sorted([e["name"] for e in d["entities"]])
