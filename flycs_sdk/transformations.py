"""Module containing transformations classes."""

from enum import Enum
from typing import List, Optional
from flycs_sdk.custom_code import Dependency
from flycs_sdk.query_base import QueryBase


class WriteDisposition(Enum):
    """Transformation write dispositions."""

    UNSPECIFIED = "UNSPECIFIED"
    WRITE_EMPTY = "WRITE_EMPTY"
    TRUNCATE = "WRITE_TRUNCATE"
    APPEND = "WRITE_APPEND"


class SchemaUpdateOptions(Enum):
    """Schema update options."""

    ALLOW_FIELD_ADDITION = "ALLOW_FIELD_ADDITION"


class ExecutionTimeoutOptions(Enum):
    """Schema update options."""

    MICROSECONDS = "microsecond "
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"
    WEEKS = "weeks"


class FieldConfig:
    """FieldConfig allows to configure special options on a single field of a table."""

    def __init__(self, field_name: str, decrypt: bool):
        """Create FieldConfig object.

        :param field_name: name of the field to configure
        :type field_name: str
        :param decrypt: whether this field should be decrypted automatically or not
        :type decrypt: bool
        """
        self.field_name = field_name
        self.decrypt = decrypt

    @classmethod
    def from_dict(cls, d: dict):
        """Create a FieldConfig object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: FieldConfig object
        :rtype: FieldConfig
        """
        return FieldConfig(field_name=d["FIELD_NAME"], decrypt=d["DECRYPT"])

    def to_dict(self) -> dict:
        """Serialize the Transformation to a dictionary object.

        :return: the FieldConfig as a dictionary object.
        :rtype: Dict
        """
        return {"FIELD_NAME": self.field_name, "DECRYPT": self.decrypt}

    def __eq__(self, other):
        """Implement __eq__ method."""
        return self.field_name == other.field_name and self.decrypt == other.decrypt


class ExecutionTimeout:
    """ExecutionTimeout allows to configure special options timeout execution."""

    def __init__(self, delta_type: str, delta: int):
        """Create ExecutionTimeout object.

        :param delta_type: name of delta type MICROSECODNS, SECONDS, MINUTES, HOURS, DAYS OR WEEKS
        :type delta_type: ExecutionTimeoutOptions
        :param delta: total delta
        :type decrypt: int
        """
        self.delta_type = delta_type
        self.delta = delta

    @classmethod
    def from_dict(cls, d: dict):
        """Create a ExecutionTimeout object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: ExecutionTimeout object
        :rtype: ExecutionTimeout
        """
        return ExecutionTimeout(delta_type=d["DELTA_TYPE"], delta=d["DELTA"])

    def to_dict(self) -> dict:
        """Serialize the Transformation to a dictionary object.

        :return: the ExecutionTimeout as a dictionary object.
        :rtype: Dict
        """
        return {"DELTA_TYPE": self.delta_type, "DELTA": self.delta}

    def __eq__(self, other):
        """Implement __eq__ method."""
        return self.delta_type == other.delta_type and self.delta == other.delta


class Transformation(QueryBase):
    """Transformations are the lowest unit inside of a data pipeline. It is a single task implemented as a SQL query."""

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
        persist_backup: Optional[bool] = True,
        write_disposition: Optional[WriteDisposition] = WriteDisposition.APPEND,
        time_partitioning: Optional[dict] = None,
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
        fields_config: Optional[List[FieldConfig]] = None,
        run_before_keyset: Optional[bool] = False,
        trigger_rule: Optional[str] = None,
        execution_timeout: Optional[ExecutionTimeout] = None,
        keysets_used: Optional[List[str]] = None,
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
        :param field_config: List of extra configuration per field of the transformation
        :type field_config: List[FieldConfig], optional
        :param run_before_keyset: override dependencies and only use the explicitly defined dependencies
        :type run_before_keyset: bool
        :param trigger_rule: set a trigger rule for the task
        :type trigger_rule: str
        :param execution_timeout_in_minutes: set an sla in minutes for the transformation
        :type execution_timeout_in_minutes: in
        :param keysets_used: List of keysets used in the transformation
        :type keysets_used: List[str]
        """
        super().__init__(
            name=name,
            query=query,
            version=version,
            encrypt=encrypt,
            static=static,
            destination_data_mart=destination_data_mart,
        )
        self.has_output = has_output
        self.destination_table = destination_table
        self.keep_old_columns = keep_old_columns
        self.persist_backup = persist_backup
        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.table_expiration = table_expiration
        self.partition_expiration = partition_expiration
        self.required_partition_filter = required_partition_filter
        self.schema_update_options = schema_update_options
        self.dependencies = dependencies or []
        self.parsing_dependencies = parsing_dependencies or []
        self.destroy_table = destroy_table
        self.tables = tables
        self.fields_config = fields_config or []
        self.run_before_keyset = run_before_keyset
        self.trigger_rule = trigger_rule
        self.execution_timeout = execution_timeout or None
        self.keysets_used = keysets_used or []

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
            persist_backup=d.get("PERSIST_BACKUP", True),
            write_disposition=WriteDisposition(
                d.get("WRITE_DISPOSITION", "WRITE_APPEND")
            ),
            time_partitioning=d.get("TIME_PARTITIONING"),
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
            fields_config=[
                FieldConfig.from_dict(x) for x in d.get("FIELDS_CONFIG") or []
            ],
            run_before_keyset=d.get("RUN_BEFORE_KEYSET"),
            trigger_rule=d.get("TRIGGER_RULE"),
            execution_timeout=(
                ExecutionTimeout(
                    ExecutionTimeoutOptions(
                        (d.get("EXECUTION_TIMEOUT").get("DELTA_TYPE")),
                        d.get("EXECUTION_TIMEOUT").get("DELTA"),
                    )
                    if d.get("EXECUTION_TIMEOUT")
                    else None
                )
            ),
            keysets_used=d.get("KEYSETS_USED", []),
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
            "FIELDS_CONFIG": [config.to_dict() for config in self.fields_config],
            "RUN_BEFORE_KEYSET": self.run_before_keyset,
            "TRIGGER_RULE": self.trigger_rule,
            "EXECUTION_TIMEOUT": self.execution_timeout.to_dict(),
            "KEYSETS_USED": self.keysets_used,
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
            and self.fields_config == other.fields_config
            and self.run_before_keyset == other.run_before_keyset
            and self.trigger_rule == other.trigger_rule
            and self.execution_timeout == other.execution_timeout
            and self.keysets_used == other.keysets_used
        )
