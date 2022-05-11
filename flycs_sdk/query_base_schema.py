"""This module contains base class for any "query" object that needs to define a schema (Transformation, views)."""

from abc import abstractmethod
from typing import List, Optional

from flycs_sdk.query_base import QueryBase

BQ_DATA_TYPES = {
    "STRING",
    "BYTES",
    "FLOAT",
    "FLOAT64",
    "BOOLEAN",
    "BOOL",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "DATETIME",
    "GEOGRAPHY",
    "INTERVAL",
    "INT",
    "INT64",
    "INTEGER",
    "BIGINT",
    "NUMERIC",
    "DECIMAL",
    "BIGNUMERIC",
    "BIGDECIMAL",
    "SMALLINT",
    "TINYINT",
    "BYTEINT",
    "RECORD",
    "STRUCT",
}
BQ_MODES = {"NULLABLE", "REPEATED", "REQUIRED"}


class UnsupportedType(Exception):
    """UnsupportedType exception raised when a data type is not supported in BigQuery."""

    def __init__(self, msg: str) -> None:
        """Create an UnsupportedType exception object."""
        super().__init__(msg)
        self.message = msg


class UnsupportedMode(Exception):
    """UnsupportedMode exception raised when a mode is not supported in BigQuery."""

    def __init__(self, msg: str) -> None:
        """Create an UnsupportedMode exception object."""
        super().__init__(msg)
        self.message = msg


class FieldConfig:
    """FieldConfig allows to configure special options on a single field of a table."""

    def __init__(
        self,
        name: str,
        type: str,
        mode: str,
        decrypt=False,
        fields: Optional[List] = None,
    ):
        """Create FieldConfig object.

        :param name: name of the field to configure
        :type name: str
        :param decrypt: whether this field should be decrypted automatically or not
        :type decrypt: bool
        :param fields: in case type is record, contains the field of the record
        :type fields: list
        """
        self.name = name
        self.type = type
        self.mode = mode
        self.decrypt = decrypt
        self.fields = fields or []
        self._validate()

    def _validate(self):
        if self.type not in BQ_DATA_TYPES:
            raise UnsupportedType(
                f"Unsupported type: {self.type} is not a supported type in BigQuery. Type should be one of: {BQ_DATA_TYPES}"
            )
        if len(self.fields) > 0 and self.type not in ["RECORD", "STRUCT"]:
            raise UnsupportedType(
                f"the field {self.name} defines some sub fields but its type is not RECORD no STRUCT."
            )

        if self.type in ["RECORD", "STRUCT"] and not self.fields:
            raise UnsupportedType(
                f"the field field {self.name} type is not RECORD or STRUCT but it does not define structure schema."
            )

        if self.mode not in BQ_MODES:
            raise UnsupportedMode(
                f"Unsupported mode: {self.mode} is not a supported type in BigQuery. Type should be one of: {BQ_MODES}"
            )

    @classmethod
    def from_dict(cls, d: dict):
        """Create a FieldConfig object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: FieldConfig object
        :rtype: FieldConfig
        """
        return FieldConfig(
            name=d.get("NAME", d.get("name")),
            decrypt=d.get("DECRYPT", d.get("decrypt", False)),
            type=d.get("TYPE", d.get("type")),
            mode=d.get("MODE", d.get("mode")),
            fields=[
                FieldConfig.from_dict(field)
                for field in d.get("FIELDS", d.get("fields")) or []
            ],
        )

    def to_dict(self) -> dict:
        """Serialize the Transformation to a dictionary object.

        :return: the FieldConfig as a dictionary object.
        :rtype: Dict
        """
        return {
            "NAME": self.name,
            "DECRYPT": self.decrypt,
            "TYPE": self.type,
            "MODE": self.mode,
            "FIELDS": [f.to_dict() for f in self.fields or []],
        }

    def __eq__(self, other):
        """Implement __eq__ method."""
        return (
            self.name == other.name
            and self.mode == other.mode
            and self.type == other.type
            and self.decrypt == other.decrypt
            and self.fields == other.fields
        )


class QueryBaseWithSchema(QueryBase):
    """Base class for any query based object that needs to have a schema defined."""

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
        schema: Optional[List[FieldConfig]] = None,
        encrypt: Optional[bool] = None,
        static: Optional[bool] = True,
        destination_data_mart: Optional[str] = None,
    ):
        """Create a QueryBase object.

        :param name: name of the transformation
        :type name: str
        :param query: SQL query
        :type query: str
        :param version: version of the tranformation
        :type version: str
        :param encrypt: if set to False, disable automatic encryption of the result of the query
        :type encrypt: Optional[bool]
        :param static: Whether or not the version should be appended to the table name, defaults to False
        :type static: bool, optional
        :param destination_data_mart: Alias of the table to use for data mart
        :type destination_data_mart: str
        """
        self.name = name
        self.query = query
        self.version = version
        self.schema = schema or []
        self.encrypt = encrypt
        self.static = static
        self.destination_data_mart = destination_data_mart

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict):
        """Create an object object form a dictionnary created with the to_dict method."""
        pass

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize the objet to a dictionary."""
        pass
