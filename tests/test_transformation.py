from flycs_sdk.transformations import (
    Transformation,
    WriteDisposition,
    SchemaUpdateOptions,
)
import pytest

transformation_name = "my_tranformation"
transformation_query = "SELECT * FROM TABLE;"
transformation_version = "1.0.0"
transformation_static = False
transformation_has_output = False
transformation_destination_table = None
transformation_write_disposition = WriteDisposition.APPEND
transformation_time_partitioning = None
transformation_cluster_fields = ["field1", "field2"]
transformation_schema_update_options = [SchemaUpdateOptions.ALLOW_FIELD_ADDITION]
transformation_dependencies = []


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
            write_disposition=transformation_write_disposition,
            time_partitioning=transformation_time_partitioning,
            cluster_fields=transformation_cluster_fields,
            schema_update_options=transformation_schema_update_options,
            dependencies=transformation_dependencies,
        )

    def test_init(self, my_transformation):
        assert my_transformation.query == transformation_query
        assert my_transformation.version == transformation_version
        assert my_transformation.static == transformation_static
        assert my_transformation.has_output == transformation_has_output
        assert my_transformation.destination_table == transformation_destination_table
        assert my_transformation.write_disposition == transformation_write_disposition
        assert my_transformation.time_partitioning == transformation_time_partitioning
        assert my_transformation.cluster_fields == transformation_cluster_fields
        assert (
            my_transformation.schema_update_options
            == transformation_schema_update_options
        )
        assert my_transformation.dependencies == transformation_dependencies

    def test_to_dict(self, my_transformation):
        assert my_transformation.to_dict() == {
            "NAME": "my_tranformation",
            "QUERY": "SELECT * FROM TABLE;",
            "VERSION": "1.0.0",
            "STATIC": False,
            "HAS_OUTPUT": False,
            "DESTINATION_TABLE": None,
            "WRITE_DISPOSITION": "WRITE_APPEND",
            "TIME_PARTITIONING": None,
            "CLUSTER_FIELDS": ["field1", "field2"],
            "SCHEMA_UPDATE_OPTIONS": ["ALLOW_FIELD_ADDITION"],
            "DEPENDS_ON": [],
            "TABLES": None,
        }

    def test_from_dict(self, my_transformation):
        loaded = Transformation.from_dict(my_transformation.to_dict())
        assert loaded == my_transformation
