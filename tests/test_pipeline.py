#!/usr/bin/env python
"""Tests for `flycs_sdk` package."""
# pylint: disable=redefined-outer-name

from datetime import datetime, timezone, timedelta

import pytest
from deepdiff import DeepDiff
from flycs_sdk.entities import Entity, ParametrizedEntity
from flycs_sdk.pipelines import (
    Pipeline,
    ParametrizedPipeline,
    PipelineKind,
    _parse_datetime,
    _format_datetime,
)

pipeline_name = "test"
pipeline_version = "1.0.0"
pipeline_schedule = "* 12 * * *"
pipeline_kind = PipelineKind.VANILLA
pipeline_start_time = datetime.fromtimestamp(1606923514, tz=timezone.utc)


class TestPipeline:
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return Entity("entity1", "1.0.0", stage_config)

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
        with pytest.raises(TypeError):
            return Pipeline(
                name=pipeline_name,
                version=pipeline_version,
                schedule=pipeline_schedule,
                kind=pipeline_kind,
                start_time=1234,
                entities=[],
            )
        with pytest.raises(ValueError):
            return Pipeline(
                name=pipeline_name,
                version=pipeline_version,
                schedule=pipeline_schedule,
                kind=pipeline_kind,
                start_time=datetime.fromtimestamp(
                    1606923514, tz=timezone(timedelta(1))
                ),
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
            "start_time": "2020-12-02T15:38:34+0000",
            "params": {},
            "entities": [
                {
                    "name": "entity1",
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
        serialized = my_pipeline.to_dict()
        if not isinstance(serialized, list):
            serialized = [serialized]
        for d in serialized:
            loaded = Pipeline.from_dict(d)
            if isinstance(my_pipeline, ParametrizedPipeline):
                assert loaded.params  # ensure the params area loaded

    def test_parse_datetime(self):
        tstr = _format_datetime(pipeline_start_time)
        parsed = _parse_datetime(tstr)
        assert parsed == pipeline_start_time


pipeline_parameters = {"language": ["nl", "fr"], "country": ["be", "en"]}


class TestParametrizedPipeline(TestPipeline):
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return ParametrizedEntity("entity1", "1.0.0", stage_config)

    @pytest.fixture
    def my_non_parameterized_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        return Entity("entity1", "1.0.0", stage_config)

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
        expected = [
            {
                "name": "test_nl_be",
                "version": "1.0.0",
                "schedule": "* 12 * * *",
                "start_time": "2020-12-02T15:38:34+0000",
                "kind": "vanilla",
                "params": {"language": "nl", "country": "be"},
                "entities": [
                    {
                        "name": "entity1_nl_be",
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
            },
            {
                "name": "test_nl_en",
                "version": "1.0.0",
                "schedule": "* 12 * * *",
                "start_time": "2020-12-02T15:38:34+0000",
                "kind": "vanilla",
                "params": {"language": "nl", "country": "en"},
                "entities": [
                    {
                        "name": "entity1_nl_en",
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
            },
            {
                "name": "test_fr_be",
                "version": "1.0.0",
                "schedule": "* 12 * * *",
                "start_time": "2020-12-02T15:38:34+0000",
                "kind": "vanilla",
                "params": {"language": "fr", "country": "be"},
                "entities": [
                    {
                        "name": "entity1_fr_be",
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
            },
            {
                "name": "test_fr_en",
                "version": "1.0.0",
                "schedule": "* 12 * * *",
                "start_time": "2020-12-02T15:38:34+0000",
                "kind": "vanilla",
                "params": {"language": "fr", "country": "en"},
                "entities": [
                    {
                        "name": "entity1_fr_en",
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
            },
        ]
        assert expected == actual

    def test_name_parameters(self, my_pipeline, my_entity):
        my_pipeline.add_entity(my_entity)
        d = my_pipeline.to_dict()
        entity_names = []
        pipeline_names = []
        for p in d:
            pipeline_names.append(p["name"])
            entity_names.extend([e["name"] for e in p["entities"]])

        assert sorted(
            ["entity1_nl_be", "entity1_nl_en", "entity1_fr_be", "entity1_fr_en"]
        ) == sorted(entity_names)
        assert sorted(
            ["test_nl_be", "test_nl_en", "test_fr_be", "test_fr_en"]
        ) == sorted(pipeline_names)
