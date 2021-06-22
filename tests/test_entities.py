#!/usr/bin/env python
"""Tests for `flycs_sdk` package."""
# pylint: disable=redefined-outer-name

import pytest
from deepdiff import DeepDiff

from flycs_sdk.entities import (
    BaseLayerEntity,
    Entity,
    ParametrizedBaseLayerEntity,
    ParametrizedEntity,
    _parametrized_name,
    ConflictingNameError,
)
from flycs_sdk.transformations import Transformation
from flycs_sdk.views import View

entity_name = "test"
entity_version = "1.0.0"


class TestEntity:
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
        }
        return Entity(entity_name, entity_version, stage_config)

    @pytest.fixture
    def my_dict(self):
        return {
            "name": entity_name,
            "version": entity_version,
            "stage_config": [
                {"name": "raw", "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},},
                {
                    "name": "staging",
                    "versions": {"table_3": "1.0.0", "table_4": "1.0.0"},
                },
            ],
        }

    def test_init(self, my_entity):
        assert my_entity.name == entity_name
        assert my_entity.version == entity_version

    def test_add_transformation(self):
        entity = Entity(entity_name, entity_version)
        transformation = Transformation("my_query", "SELECT * FROM TABLE", "1.0.0")
        entity.add_transformation("staging", transformation)
        assert entity.stage_config == {
            "staging": {transformation.name: transformation.version}
        }
        assert entity.transformations == {
            "staging": {transformation.name: transformation}
        }

        with pytest.raises(ConflictingNameError):
            entity.add_transformation("staging", transformation)

    def test_add_view(self):
        entity = Entity(entity_name, entity_version)
        view = View("my_query", "SELECT * FROM TABLE", "1.0.0")
        entity.add_view("staging", view)
        assert entity.stage_config == {"staging": {view.name: view.version}}
        assert entity.transformations == {"staging": {view.name: view}}

        with pytest.raises(ConflictingNameError):
            entity.add_view("staging", view)

    def test_to_dict(self, my_entity):
        assert not DeepDiff(
            my_entity.to_dict(),
            {
                "name": entity_name,
                "version": entity_version,
                "stage_config": [
                    {
                        "name": "raw",
                        "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                    },
                    {
                        "name": "staging",
                        "versions": {"table_3": "1.0.0", "table_4": "1.0.0"},
                    },
                ],
            },
            ignore_order=True,
        )

    def test_from_dict(self, my_dict):
        e = Entity.from_dict(my_dict)
        assert e.name == entity_name
        assert e.version == entity_version
        assert e.stage_config == {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
        }

    def test_serialize_deserialize(self, my_entity):
        d = my_entity.to_dict()
        loaded = Entity.from_dict(d)
        assert loaded == my_entity


class TestBaseLayerEntity(TestEntity):
    @pytest.fixture
    def empty_entity(self):
        return BaseLayerEntity(entity_name, entity_version,)

    @pytest.fixture
    def my_entity(self):
        return BaseLayerEntity(
            entity_name,
            entity_version,
            datalake_versions={"table_1": "1.0.0", "table_2": "1.0.0"},
            preamble_versions={"table_3": "1.0.0", "table_4": "1.0.0"},
            staging_versions={"table_5": "1.0.0", "table_6": "1.0.0"},
            data_warehouse_versions={"table_7": "1.0.0", "table_8": "1.0.0"},
            data_mart_versions={"table_9": "1.0.0", "table_10": "1.0.0"},
        )

    @pytest.fixture
    def my_dict(self):
        return {
            "name": entity_name,
            "version": entity_version,
            "stage_config": [
                {
                    "name": "datalake",
                    "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                },
                {
                    "name": "preamble",
                    "versions": {"table_3": "1.0.0", "table_4": "1.0.0"},
                },
                {
                    "name": "staging",
                    "versions": {"table_5": "1.0.0", "table_6": "1.0.0"},
                },
                {
                    "name": "data_warehouse",
                    "versions": {"table_7": "1.0.0", "table_8": "1.0.0"},
                },
                {
                    "name": "data_mart",
                    "versions": {"table_9": "1.0.0", "table_10": "1.0.0"},
                },
            ],
        }

    def test_to_dict(self, my_entity):
        assert not DeepDiff(
            my_entity.to_dict(),
            {
                "name": entity_name,
                "version": entity_version,
                "stage_config": [
                    {
                        "name": "datalake",
                        "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                    },
                    {
                        "name": "preamble",
                        "versions": {"table_3": "1.0.0", "table_4": "1.0.0"},
                    },
                    {
                        "name": "staging",
                        "versions": {"table_5": "1.0.0", "table_6": "1.0.0"},
                    },
                    {
                        "name": "data_warehouse",
                        "versions": {"table_7": "1.0.0", "table_8": "1.0.0"},
                    },
                    {
                        "name": "data_mart",
                        "versions": {"table_9": "1.0.0", "table_10": "1.0.0"},
                    },
                ],
            },
            ignore_order=True,
        )

    def test_to_dict_empty(self, empty_entity):
        assert not DeepDiff(
            empty_entity.to_dict(),
            {
                "name": entity_name,
                "version": entity_version,
                "stage_config": [
                    {"name": "datalake", "versions": {}},
                    {"name": "preamble", "versions": {}},
                    {"name": "staging", "versions": {}},
                    {"name": "data_warehouse", "versions": {}},
                    {"name": "data_mart", "versions": {}},
                ],
            },
            ignore_order=True,
        )

    def test_from_dict(self, my_dict):
        e = BaseLayerEntity.from_dict(my_dict)
        assert e.name == entity_name
        assert e.version == entity_version
        assert e.datalake_versions == {"table_1": "1.0.0", "table_2": "1.0.0"}
        assert e.preamble_versions == {"table_3": "1.0.0", "table_4": "1.0.0"}
        assert e.staging_versions == {"table_5": "1.0.0", "table_6": "1.0.0"}
        assert e.data_warehouse_versions == {"table_7": "1.0.0", "table_8": "1.0.0"}
        assert e.data_mart_versions == {"table_9": "1.0.0", "table_10": "1.0.0"}
        assert e.stage_config == {
            "datalake": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "preamble": {"table_3": "1.0.0", "table_4": "1.0.0"},
            "staging": {"table_5": "1.0.0", "table_6": "1.0.0"},
            "data_warehouse": {"table_7": "1.0.0", "table_8": "1.0.0"},
            "data_mart": {"table_9": "1.0.0", "table_10": "1.0.0"},
        }

    def test_serialize_deserialize(self, my_entity):
        d = my_entity.to_dict()
        loaded = BaseLayerEntity.from_dict(d)
        assert loaded == my_entity


class TestParametrizedEntity(TestEntity):
    @pytest.fixture
    def my_entity(self):
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
        }
        return ParametrizedEntity(entity_name, entity_version, stage_config)


class TestParametrizedBaseLayerEntity(TestParametrizedEntity, TestBaseLayerEntity):
    @pytest.fixture
    def empty_entity(self):
        return ParametrizedBaseLayerEntity(entity_name, entity_version,)

    @pytest.fixture
    def my_entity(self):
        return ParametrizedBaseLayerEntity(
            entity_name,
            entity_version,
            datalake_versions={"table_1": "1.0.0", "table_2": "1.0.0"},
            preamble_versions={"table_3": "1.0.0", "table_4": "1.0.0"},
            staging_versions={"table_5": "1.0.0", "table_6": "1.0.0"},
            data_warehouse_versions={"table_7": "1.0.0", "table_8": "1.0.0"},
            data_mart_versions={"table_9": "1.0.0", "table_10": "1.0.0"},
        )


class TestParametrizedEntityName:
    def test_parametrized_entity_name(self):
        assert (
            _parametrized_name("name", {"language": "fr", "country": "be"})
            == "name_fr_be"
        )
