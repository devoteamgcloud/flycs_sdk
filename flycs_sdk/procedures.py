"""Module containing stored_procedure classes."""

from typing import Optional, List

from flycs_sdk.custom_code import Dependency
from flycs_sdk.query_base import QueryBase


class Argument:
    """Class representing a Stored Procedure Argument."""

    def __init__(self, name: str, type: str, mode: Optional[str]):
        """Create an Argument object.

        :param name: name of the argument
        :type name: str
        :param type: the SQL type of the argument
        :type type: str
        :param type: the SQL mode of the argumment (IN , OUT or INOUT)
        :type mode: str
        """
        self.name = name
        self.type = type
        self.mode = mode

    def to_dict(self) -> dict:
        """
        Serialize the Argument to a dictionary object.

        :return: the Argument as a dictionary object.
        """
        return {"NAME": self.name, "TYPE": self.type, "MODE": self.mode}

    @classmethod
    def from_dict(cls, a):
        """Create an Argument object form a dictionary created with the to_dict method.

        :param a: source dictionary
        :type a: dict
        :return: Argument
        :rtype: Argument
        """
        return cls(name=a["NAME"], type=a["TYPE"], mode=a["MODE"])

    def __eq__(self, o) -> bool:
        """Implement __eq__ method."""
        return self.name == o.name and self.type == o.type and self.mode == o.type


class StoredProcedure(QueryBase):
    """Class representing a Stored Procedure configuration."""

    kind = "stored_procedure"

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
        argument_list: List[Argument],
        return_type: Optional[str] = None,
        language: Optional[str] = "sql",
        description: Optional[str] = None,
        static: Optional[bool] = True,
        destination_data_mart: Optional[str] = None,
    ):
        """Create a stored_procedure object.

        :param name: name of the Stored Procedure
        :type name: str
        :param query: SQL body of the Stored Procedure
        :type query: str
        :param version: version of the Stored Procedure
        :type version: str
        :param argument_list: the list of arguments of the Stored Procedure
        :type argument_list: List[Argument]
        :param return_type: the SQL return type of the Stored Procedure
        :type return_type: Optional[str]
        :param language: the language of the Stored Procedure, defaults to sql
        :type language: Optional[str]
        :param description: description of the Stored Procedure, defaults to None
        :type description: Optional[str], optional
        """
        super().__init__(
            name=name,
            query=query,
            version=version,
            static=static,
            destination_data_mart=destination_data_mart,
        )
        self.destination_table = None
        self.dependencies = []
        self.parsing_dependencies = []
        self.argument_list = argument_list
        self.description = description
        self.return_type = return_type
        self.language = language

    @classmethod
    def from_dict(cls, d: dict):
        """Create a Stored Procedure object from a dictionary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: StoredProcedure
        :rtype: StoredProcedure
        """
        stored_procedure = cls(
            name=d.get("NAME", ""),
            query=d["QUERY"],
            version=d["VERSION"],
            description=d.get("DESCRIPTION"),
            static=d.get("STATIC", True),
            destination_data_mart=d.get("DESTINATION_DATA_MART"),
            argument_list=[Argument.from_dict(a) for a in d.get("ARGUMENT_LIST") or []],
            return_type=d.get("RETURN_TYPE"),
            language=d.get("LANGUAGE", "sql"),
        )
        stored_procedure.destination_table = d.get("DESTINATION_TABLE")
        stored_procedure.dependencies = [
            Dependency.from_dict(x) for x in d.get("DEPENDS_ON") or []
        ]
        stored_procedure.parsing_dependencies = [
            Dependency.from_dict(x) for x in d.get("PARSING_DEPENDS_ON") or []
        ]
        return stored_procedure

    def to_dict(self) -> dict:
        """
        Serialize the Stored Procedure to a dictionary object.

        :return: the Stored Procedure as a dictionary object.
        :rtype: Dict
        """
        return {
            "NAME": self.name,
            "QUERY": self.query,
            "VERSION": self.version,
            "DESCRIPTION": self.description,
            "DESTINATION_TABLE": self.destination_table,
            "KIND": self.kind,
            "STATIC": self.static,
            "DESTINATION_DATA_MART": self.destination_data_mart,
            "DEPENDS_ON": [d.to_dict() for d in self.dependencies],
            "PARSING_DEPENDS_ON": [d.to_dict() for d in self.parsing_dependencies],
            "ARGUMENT_LIST": [a.to_dict() for a in self.argument_list],
            "RETURN_TYPE": self.return_type,
            "LANGUAGE": self.language,
        }

    def __eq__(self, o) -> bool:
        """Implement __eq__ method."""
        return (
            self.name == o.name
            and self.query == o.query
            and self.version == o.version
            and self.description == o.description
            and self.destination_table == o.destination_table
            and self.kind == o.kind
            and self.static == o.static
            and self.destination_data_mart == o.destination_data_mart
            and self.dependencies == o.dependencies
            and self.parsing_dependencies == o.parsing_dependencies
            and self.argument_list == o.argument_list
            and self.return_type == o.return_type
            and self.language == o.language
        )
