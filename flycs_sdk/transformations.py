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
        static: bool = True,
        has_output: bool = False,
        destination_table: str = None,
        keep_old_columns: bool = True,
        persist_backup: bool = True,
        write_disposition: WriteDisposition = WriteDisposition.APPEND,
        time_partitioning: dict = None,
        cluster_fields: List[str] = None,
        schema_update_options: List[SchemaUpdateOptions] = [
            SchemaUpdateOptions.ALLOW_FIELD_ADDITION
        ],
        destination_data_mart: str = None,
        dependencies: List[dict] = None,
        parsing_dependencies: List[dict] = None,
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
        :param keep_old_columns:
        :type keep_old_columns: bool, optional
        :param persist_backup:
        :type persist_backup: bool, optional
        :param write_disposition: The write disposition for writing the results, defaults to WriteDisposition.APPEND
        :type write_disposition: WriteDisposition, optional
        :param time_partitioning: An dictionary with the time partition configuration, defaults to None
        :type time_partitioning: dict, optional
        :param cluster_fields: An ordered list of the columns you want to cluster on., defaults to None
        :type cluster_fields: List[str], optional
        :param schema_update_options: Whether or not the schema can be changed by adding fields, defaults to [ SchemaUpdateOptions.ALLOW_FIELD_ADDITION ]
        :type schema_update_options: List[SchemaUpdateOptions], optional
        :param destination_data_mart: Alias of the table to use for data mart
        :type destination_data_mart: str
        :param dependencies: This allows you to set hard dependencies on your queries, defaults to None
        :type dependencies: List[dict], optional
        :param parsing_dependencies:
        :type parsing_dependencies: List[dict], optional
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
        self.keep_old_columns = keep_old_columns
        self.persist_backup = persist_backup
        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.schema_update_options = schema_update_options
        self.destination_data_mart = destination_data_mart
        self.dependencies = dependencies
        self.parsing_dependencies = parsing_dependencies
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
            name=d.get("NAME", ""),
            query=d["QUERY"],
            version=d["VERSION"],
            static=d.get("STATIC", True),
            has_output=d.get("HAS_OUTPUT", True),
            destination_table=d.get("DESTINATION_TABLE"),
            keep_old_columns=d.get("KEEP_OLD_COLUMNS", True),
            persist_backup=d.get("PERSIST_BACKUP", True),
            write_disposition=WriteDisposition(
                d.get("WRITE_DISPOSITION", "WRITE_APPEND")
            ),
            time_partitioning=d.get("TIME_PARTITIONING"),
            cluster_fields=d.get("CLUSTER_FIELDS"),
            schema_update_options=[
                SchemaUpdateOptions(x) for x in d.get("SCHEMA_UPDATE_OPTIONS", [])
            ],
            destination_data_mart=d.get("DESTINATION_DATA_MART"),
            dependencies=d.get("DEPENDS_ON", []),
            parsing_dependencies=d.get("PARSING_DEPENDS_ON", []),
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
            "KEEP_OLD_COLUMNS": self.keep_old_columns,
            "PERSIST_BACKUP": self.persist_backup,
            "WRITE_DISPOSITION": self.write_disposition.value,
            "TIME_PARTITIONING": self.time_partitioning,
            "CLUSTER_FIELDS": self.cluster_fields,
            "SCHEMA_UPDATE_OPTIONS": [
                o.value for o in self.schema_update_options or []
            ],
            "DESTINATION_DATA_MART": self.destination_data_mart,
            "DEPENDS_ON": self.dependencies,
            "PARSING_DEPENDS_ON": self.parsing_dependencies,
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
            and self.keep_old_columns == other.keep_old_columns
            and self.persist_backup == other.persist_backup
            and self.write_disposition == other.write_disposition
            and self.time_partitioning == other.time_partitioning
            and self.cluster_fields == other.cluster_fields
            and self.schema_update_options == other.schema_update_options
            and self.destination_data_mart == other.destination_data_mart
            and self.dependencies == other.dependencies
            and self.parsing_dependencies == other.parsing_dependencies
            and self.tables == other.tables
        )
