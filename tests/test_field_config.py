import pytest


from flycs_sdk.query_base_schema import UnsupportedMode, UnsupportedType
from flycs_sdk.transformations import FieldConfig


class TestFieldConfig:
    def test_invalid_type(self):
        with pytest.raises(UnsupportedType):
            FieldConfig(name="field1", decrypt=False, type="STRING3", mode="NULLABLE")

    def test_invalid_mode(self):
        with pytest.raises(UnsupportedMode):
            FieldConfig(name="field1", decrypt=False, type="STRING", mode="NULLABLEz")

    def test_record_type(self):
        with pytest.raises(UnsupportedType):
            FieldConfig(name="field1", decrypt=False, type="STRUCT", mode="NULLABLE")
        with pytest.raises(UnsupportedType):
            FieldConfig(name="field1", decrypt=False, type="RECORD", mode="NULLABLE")
        with pytest.raises(UnsupportedType):
            FieldConfig(
                name="field1",
                decrypt=False,
                type="STRING",
                mode="NULLABLE",
                fields=[
                    FieldConfig(name="level1", type="STRING", mode="NULLABLE"),
                ],
            )

        FieldConfig(
            name="top_level",
            decrypt=False,
            type="RECORD",
            mode="NULLABLE",
            fields=[
                FieldConfig(name="level1", type="STRING", mode="NULLABLE"),
                FieldConfig(
                    name="level1",
                    type="RECORD",
                    mode="NULLABLE",
                    fields=[
                        FieldConfig(name="level2", type="STRING", mode="NULLABLE"),
                    ],
                ),
            ],
        )
