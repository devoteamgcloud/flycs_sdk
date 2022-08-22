"""Module containing transformations classes."""


from enum import Enum
from typing import List, Optional

from flycs_sdk.custom_code import Dependency
from flycs_sdk.query_base_schema import QueryBaseWithSchema, FieldConfig


class WriteDisposition(Enum):
    """Transformation write dispositions."""

    UNSPECIFIED = "UNSPECIFIED"
    WRITE_EMPTY = "WRITE_EMPTY"
    TRUNCATE = "WRITE_TRUNCATE"
    APPEND = "WRITE_APPEND"


class SchemaUpdateOptions(Enum):
    """Schema update options."""

    ALLOW_FIELD_ADDITION = "ALLOW_FIELD_ADDITION"


class MultiPartitioningDefinitionError(Exception):
    """Raised when trying to create a Transformation object with time partitioning & range partitioning."""

    pass


class Transformation(QueryBaseWithSchema):
    """Transformations are the lowest unit inside a data pipeline. It is a single task implemented as a SQL query."""

    kind = "transformation"

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
        encrypt: Optional[bool] = None,
        static: Optional[bool] = True,
        has_output: Optional[bool] = False,
        destination_table: Optional[str] = None,
        keep_old_columns: Optional[bool] = True,
        persist_backup: Optional[bool] = None,
        write_disposition: Optional[WriteDisposition] = WriteDisposition.APPEND,
        time_partitioning: Optional[dict] = None,
        range_partitioning: Optional[dict] = None,
        cluster_fields: Optional[List[str]] = None,
        table_expiration: Optional[int] = None,
        partition_expiration: Optional[int] = None,
        required_partition_filter: Optional[bool] = False,
        schema_update_options: Optional[List[SchemaUpdateOptions]] = [
            SchemaUpdateOptions.ALLOW_FIELD_ADDITION
        ],
        destination_data_mart: Optional[str] = None,
        dependencies: Optional[List[Dependency]] = None,
        parsing_dependencies: Optional[List[Dependency]] = None,
        destroy_table: Optional[bool] = False,
        tables: Optional[List[dict]] = None,
        schema: Optional[List[FieldConfig]] = None,
        force_cache_refresh: Optional[bool] = False,
    ):
        """Class representing a transformation.

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
        :param range_partitioning: A dictionary with the range partition configuration , defaults to None (time_partitioning must be None)
        :type range_partitioning: dict, optional
        :param cluster_fields: An ordered list of the columns you want to cluster on., defaults to None
        :type cluster_fields: List[str], optional
        :param table_expiration: Optional number of day before expiration for the table generated by this transformation
        :type table_expiration: int
        :param partition_expiration: Optional number of day before the expiration of the partition used by the table generated by this transformation
        :type partition_expiration: int
        :param required_partition_filter: whether or not partition filter is required on the table generated by this transformation
        :type required_partition_filter: bool
        :param schema_update_options: Whether or not the schema can be changed by adding fields, defaults to [ SchemaUpdateOptions.ALLOW_FIELD_ADDITION ]
        :type schema_update_options: List[SchemaUpdateOptions], optional
        :param destination_data_mart: Alias of the table to use for data mart
        :type destination_data_mart: str
        :param dependencies: This allows you to set hard dependencies on your queries, defaults to None
        :type dependencies: List[Dependency], optional
        :param parsing_dependencies:
        :type parsing_dependencies: List[Dependency], optional
        :param destroy_table: If True, the resulting table of this transformation will be destroy and recreated automatically. Only works in sandbox environment.
        :type destroy_table: bool, default to False
        :param tables: If specified, this transformation will generate multiple BigQueryOperator during Airflow generation, one for each table name in this list.
                       The name of the transformation then becomes `{transformation_name}_{table_name}`
        :type tables: List[str], optional
        :param schema: List of extra configuration per field of the transformation
        :type schema: List[FieldConfig], optional
        :param force_cache_refresh: whether or not we need to use the cache in the pii service
        :type force_cache_refresh: bool, optional
        """
        super().__init__(
            name=name,
            query=query,
            version=version,
            encrypt=encrypt,
            static=static,
            destination_data_mart=destination_data_mart,
            schema=schema or [],
        )
        self.has_output = has_output
        self.destination_table = destination_table
        self.keep_old_columns = keep_old_columns
        self.persist_backup = persist_backup
        self.write_disposition = write_disposition
        if time_partitioning and range_partitioning:
            raise MultiPartitioningDefinitionError(
                "Defining time partitioning & range partitioning is not allowed, you must choose one option between them."
            )
        self.time_partitioning = time_partitioning
        self.range_partitioning = range_partitioning
        self.cluster_fields = cluster_fields
        self.table_expiration = table_expiration
        self.partition_expiration = partition_expiration
        self.required_partition_filter = required_partition_filter
        self.schema_update_options = schema_update_options
        self.dependencies = dependencies or []
        self.parsing_dependencies = parsing_dependencies or []
        self.destroy_table = destroy_table
        self.tables = tables
        self.force_cache_refresh = force_cache_refresh

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
            encrypt=d.get("ENCRYPT", None),
            static=d.get("STATIC", True),
            has_output=d.get("HAS_OUTPUT", True),
            destination_table=d.get("DESTINATION_TABLE"),
            keep_old_columns=d.get("KEEP_OLD_COLUMNS", True),
            persist_backup=d.get("PERSIST_BACKUP"),
            write_disposition=WriteDisposition(
                d.get("WRITE_DISPOSITION", "WRITE_APPEND")
            ),
            time_partitioning=d.get("TIME_PARTITIONING"),
            range_partitioning=d.get("RANGE_PARTITIONING"),
            cluster_fields=d.get("CLUSTER_FIELDS"),
            table_expiration=d.get("TABLE_EXPIRATION"),
            partition_expiration=d.get("PARTITION_EXPIRATION"),
            required_partition_filter=d.get("REQUIRED_PARTITION_FILTER", False),
            schema_update_options=[
                SchemaUpdateOptions(x) for x in d.get("SCHEMA_UPDATE_OPTIONS", [])
            ],
            destination_data_mart=d.get("DESTINATION_DATA_MART"),
            dependencies=[Dependency.from_dict(x) for x in d.get("DEPENDS_ON") or []],
            parsing_dependencies=[
                Dependency.from_dict(x) for x in d.get("PARSING_DEPENDS_ON") or []
            ],
            destroy_table=d.get("DESTROY_TABLE", False),
            tables=d.get("TABLES"),
            schema=[FieldConfig.from_dict(x) for x in d.get("SCHEMA") or []],
            force_cache_refresh=d.get("FORCE_CACHE_REFRESH", False),
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
            "ENCRYPT": self.encrypt,
            "STATIC": self.static,
            "HAS_OUTPUT": self.has_output,
            "DESTINATION_TABLE": self.destination_table,
            "KEEP_OLD_COLUMNS": self.keep_old_columns,
            "PERSIST_BACKUP": self.persist_backup,
            "WRITE_DISPOSITION": self.write_disposition.value,
            "TIME_PARTITIONING": self.time_partitioning,
            "RANGE_PARTITIONING": self.range_partitioning,
            "CLUSTER_FIELDS": self.cluster_fields,
            "TABLE_EXPIRATION": self.table_expiration,
            "PARTITION_EXPIRATION": self.partition_expiration,
            "REQUIRED_PARTITION_FILTER": self.required_partition_filter,
            "SCHEMA_UPDATE_OPTIONS": [
                o.value for o in self.schema_update_options or []
            ],
            "DESTINATION_DATA_MART": self.destination_data_mart,
            "DEPENDS_ON": [d.to_dict() for d in self.dependencies],
            "PARSING_DEPENDS_ON": [d.to_dict() for d in self.parsing_dependencies],
            "DESTROY_TABLE": self.destroy_table,
            "TABLES": self.tables,
            "KIND": self.kind,
            "SCHEMA": [config.to_dict() for config in self.schema],
            "FORCE_CACHE_REFRESH": self.force_cache_refresh,
        }

    def __eq__(self, other):
        """Implement __eq__ method."""
        return (
            self.name == other.name
            and self.query == other.query
            and self.version == other.version
            and self.static == other.static
            and self.encrypt == other.encrypt
            and self.has_output == other.has_output
            and self.destination_table == other.destination_table
            and self.keep_old_columns == other.keep_old_columns
            and self.persist_backup == other.persist_backup
            and self.write_disposition == other.write_disposition
            and self.time_partitioning == other.time_partitioning
            and self.range_partitioning == other.range_partitioning
            and self.cluster_fields == other.cluster_fields
            and self.table_expiration == other.table_expiration
            and self.partition_expiration == other.partition_expiration
            and self.required_partition_filter == other.required_partition_filter
            and self.schema_update_options == other.schema_update_options
            and self.destination_data_mart == other.destination_data_mart
            and self.dependencies == other.dependencies
            and self.parsing_dependencies == other.parsing_dependencies
            and self.destroy_table == other.destroy_table
            and self.tables == other.tables
            and self.kind == other.kind
            and self.schema == other.schema
            and self.force_cache_refresh == other.force_cache_refresh
            and self.schema == other.schema
        )
