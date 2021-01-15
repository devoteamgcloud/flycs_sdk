"""Module containing transformations classes."""

from enum import Enum
from typing import List


class WriteDisposition(Enum):
    """Transformation write dispositions."""

    APPEND = "WRITE_APPEND"
    TRUNCATE = "WRITE_TRUNCATE"


class SchemaUpdateOptions(Enum):
    """Schema update options."""

    ALLOW_FIELD_ADDITION = "ALLOW_FIELD_ADDITION"


class Transformation:
    """Transformations are the lowest unit inside of a data pipeline. It is a single task implemented as a SQL query."""

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
        static: bool = False,
        has_output: bool = False,
        destination_table: str = None,
        write_disposition: WriteDisposition = WriteDisposition.APPEND,
        time_partitioning: dict = None,
        cluster_fields: List[str] = None,
        schema_update_options: List[SchemaUpdateOptions] = [
            SchemaUpdateOptions.ALLOW_FIELD_ADDITION
        ],
        dependencies: List[dict] = None,
        tables: List[dict] = None,
    ):
        """Class representing a transformation.

        :param name: name of the transformation
        :type name: str
        :param query: SQL query
        :type query: str
        :param version: version of the tranformation
        :type version: str
        :param static: Whether or not the version should be appended to the table name, defaults to False
        :type static: bool, optional
        :param has_output: Whether or not this query has a result that should be written into a table (false can be used to run DML or stored procedures for example), defaults to False
        :type has_output: bool, optional
        :param destination_table: [description], defaults to None
        :type destination_table: str, optional
        :param write_disposition: The write disposition for writing the results, defaults to WriteDisposition.APPEND
        :type write_disposition: WriteDisposition, optional
        :param time_partitioning: An dictionary with the time partition configuration, defaults to None
        :type time_partitioning: dict, optional
        :param cluster_fields: An ordered list of the columns you want to cluster on., defaults to None
        :type cluster_fields: List[str], optional
        :param schema_update_options: Whether or not the schema can be changed by adding fields, defaults to [ SchemaUpdateOptions.ALLOW_FIELD_ADDITION ]
        :type schema_update_options: List[SchemaUpdateOptions], optional
        :param dependencies: This allows you to set hard dependencies on your queries, defaults to None
        :type dependencies: List[dict], optional
        :param tables: If specified, this transformation will generate multiple BigQueryOperator during Airflow generation, one for each table name in this list.
                       The name of the transformation then becomes `{transformation_name}_{table_name}`
        :type tables: List[str], optional
        """
        self.name = name
        self.query = query
        self.version = version
        self.static = static
        self.has_output = has_output
        self.destination_table = destination_table
        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.schema_update_options = schema_update_options
        self.dependencies = dependencies
        self.tables = tables

    @classmethod
    def from_dict(cls, d: dict):
        """Create a Transformation object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: Transformation
        :rtype: Transformation
        """
        return Transformation(
            name=d["NAME"],
            query=d["QUERY"],
            version=d["VERSION"],
            static=d.get("STATIC", False),
            has_output=d.get("HAS_OUTPUT", False),
            destination_table=d.get("DESTINATION_TABLE"),
            write_disposition=WriteDisposition(d.get("WRITE_DISPOSITION")),
            time_partitioning=d.get("TIME_PARTITIONING"),
            cluster_fields=d.get("CLUSTER_FIELDS", []),
            schema_update_options=[
                SchemaUpdateOptions(x) for x in d["SCHEMA_UPDATE_OPTIONS"]
            ],
            dependencies=d.get("DEPENDS_ON", []),
            tables=d.get("TABLES"),
        )

    def to_dict(self) -> dict:
        """
        Serialize the Transformation to a dictionary object.

        :return: the transformation as a dictionary object.
        :rtype: Dict
        """
        return {
            "NAME": self.name,
            "QUERY": self.query,
            "VERSION": self.version,
            "STATIC": self.static,
            "HAS_OUTPUT": self.has_output,
            "DESTINATION_TABLE": self.destination_table,
            "WRITE_DISPOSITION": self.write_disposition.value,
            "TIME_PARTITIONING": self.time_partitioning,
            "CLUSTER_FIELDS": self.cluster_fields,
            "SCHEMA_UPDATE_OPTIONS": [o.value for o in self.schema_update_options],
            "DEPENDS_ON": self.dependencies,
            "TABLES": self.tables,
        }

    def __eq__(self, other):
        """Implement __eq__ method."""
        return (
            self.name == other.name
            and self.query == other.query
            and self.version == other.version
            and self.static == other.static
            and self.has_output == other.has_output
            and self.destination_table == other.destination_table
            and self.write_disposition == other.write_disposition
            and self.time_partitioning == other.time_partitioning
            and self.cluster_fields == other.cluster_fields
            and self.schema_update_options == other.schema_update_options
            and self.dependencies == other.dependencies
            and self.tables == other.tables
        )
