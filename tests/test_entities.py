#!/usr/bin/env python
"""Tests for `flycs_sdk` package."""
# pylint: disable=redefined-outer-name

import pytest
from flycs_sdk.entities import Entity, BaseLayerEntity


class TestEntity:
    def test_init(self):
        name = "test"
        version = "1.0.0"
        my_entity = Entity(name, version)
        assert my_entity.name == name
        assert my_entity.version == version

    def test_to_dict(self):
        name = "test"
        version = "1.0.0"
        stage_config = {
            "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
            "staging": {"table_1": "1.0.0", "table_2": "1.0.0"},
        }
        my_entity = Entity(name, version, stage_config=stage_config)
        assert my_entity.to_dict() == {
            "name": name,
            "version": version,
            "stage_config": [
                {"name": "raw", "versions": {"table_1": "1.0.0", "table_2": "1.0.0"}},
                {
                    "name": "staging",
                    "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                },
            ],
        }


class TestBaseLayerEntity:
    def test_init(self):
        name = "test"
        version = "1.0.0"
        my_entity = BaseLayerEntity(name, version)
        assert my_entity.name == name
        assert my_entity.version == version

    def test_to_dict(self):
        name = "test"
        version = "1.0.0"
        my_entity = BaseLayerEntity(
            name,
            version,
            datalake_versions={"table_1": "1.0.0", "table_2": "1.0.0"},
            staging_versions={"table_1": "1.0.0", "table_2": "1.0.0"},
        )
        assert my_entity.to_dict() == {
            "name": name,
            "version": version,
            "stage_config": [
                {
                    "name": "datalake",
                    "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                },
                {"name": "preamble", "versions": {}},
                {
                    "name": "staging",
                    "versions": {"table_1": "1.0.0", "table_2": "1.0.0"},
                },
                {"name": "data_warehouse", "versions": {}},
                {"name": "data_mart", "versions": {}},
            ],
        }
