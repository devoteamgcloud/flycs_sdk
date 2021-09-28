from flycs_sdk.custom_code import Dependency
from flycs_sdk.transformations import (
    FieldConfig,
    Transformation,
    WriteDisposition,
    SchemaUpdateOptions,
    FieldConfig,
)
import pytest

transformation_name = "my_tranformation"
transformation_query = "SELECT * FROM TABLE;"
transformation_version = "1.0.0"
transformation_static = False
transformation_has_output = False
transformation_destination_table = None
transformation_keep_old_columns = True
transformation_persist_backup = True
transformation_write_disposition = WriteDisposition.APPEND
transformation_time_partitioning = None
transformation_cluster_fields = ["field1", "field2"]
transformation_schema_update_options = [SchemaUpdateOptions.ALLOW_FIELD_ADDITION]
transformation_dependencies = [Dependency("entity1", "staging", "deps")]
transformation_parsing_dependencies = []
transformation_destroy_table = False
transformation_fields_config = [
    FieldConfig(field_name="field1", decrypt=False),
    FieldConfig(field_name="field2", decrypt=True),
]


class TestTranformations:
    @pytest.fixture()
    def my_transformation(self):
        return Transformation(
            name=transformation_name,
            query=transformation_query,
            version=transformation_version,
            static=transformation_static,
            has_output=transformation_has_output,
            destination_table=transformation_destination_table,
            keep_old_columns=transformation_keep_old_columns,
            persist_backup=transformation_persist_backup,
            write_disposition=transformation_write_disposition,
            time_partitioning=transformation_time_partitioning,
            cluster_fields=transformation_cluster_fields,
            schema_update_options=transformation_schema_update_options,
            dependencies=transformation_dependencies,
            parsing_dependencies=transformation_parsing_dependencies,
            destroy_table=transformation_destroy_table,
            fields_config=transformation_fields_config,
        )

    def test_init(self, my_transformation):
        assert my_transformation.query == transformation_query
        assert my_transformation.version == transformation_version
        assert my_transformation.static == transformation_static
        assert my_transformation.has_output == transformation_has_output
        assert my_transformation.keep_old_columns == transformation_keep_old_columns
        assert my_transformation.persist_backup == transformation_persist_backup
        assert my_transformation.destination_table == transformation_destination_table
        assert my_transformation.write_disposition == transformation_write_disposition
        assert my_transformation.time_partitioning == transformation_time_partitioning
        assert my_transformation.cluster_fields == transformation_cluster_fields
        assert (
            my_transformation.schema_update_options
            == transformation_schema_update_options
        )
        assert my_transformation.dependencies == transformation_dependencies
        assert my_transformation.destroy_table == transformation_destroy_table
        assert my_transformation.fields_config == transformation_fields_config

    def test_to_dict(self, my_transformation):
        assert my_transformation.to_dict() == {
            "NAME": "my_tranformation",
            "QUERY": "SELECT * FROM TABLE;",
            "VERSION": "1.0.0",
            "ENCRYPT": None,
            "STATIC": False,
            "HAS_OUTPUT": False,
            "DESTINATION_TABLE": None,
            "KEEP_OLD_COLUMNS": True,
            "PERSIST_BACKUP": True,
            "WRITE_DISPOSITION": "WRITE_APPEND",
            "TIME_PARTITIONING": None,
            "CLUSTER_FIELDS": ["field1", "field2"],
            "PARTITION_EXPIRATION": None,
            "REQUIRED_PARTITION_FILTER": False,
            "TABLE_EXPIRATION": None,
            "SCHEMA_UPDATE_OPTIONS": ["ALLOW_FIELD_ADDITION"],
            "DESTINATION_DATA_MART": None,
            "DEPENDS_ON": [{"NAME": "deps", "ENTITY": "entity1", "STAGE": "staging"}],
            "PARSING_DEPENDS_ON": [],
            "DESTROY_TABLE": False,
            "TABLES": None,
            "KIND": "transformation",
            "FIELDS_CONFIG": [
                {"FIELD_NAME": "field1", "DECRYPT": False,},
                {"FIELD_NAME": "field2", "DECRYPT": True,},
            ],
        }

    def test_from_dict(self, my_transformation):
        loaded = Transformation.from_dict(my_transformation.to_dict())
        assert loaded == my_transformation
