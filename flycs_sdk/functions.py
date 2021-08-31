"""Module containing view classes."""

from typing import Optional, List

from flycs_sdk.custom_code import Dependency
from flycs_sdk.query_base import QueryBase


class Argument:
    """Class representing a function Argument."""

    def __init__(self, name: str, type: str):
        """Create an Argument object.

        :param name: name of the argument
        :type name: str
        :param type: the SQL type of the argument
        :type type: str
        """
        self.name = name
        self.type = type

    def to_dict(self):
        """
        Serialize the Argument to a dictionary object.

        :return: the Argument as a dictionary object.
        """
        return {"NAME": self.name, "TYPE": self.type}

    @classmethod
    def from_dict(cls, a):
        """Create an Argument object form a dictionary created with the to_dict method.

        :param a: source dictionary
        :type a: dict
        :return: Argument
        :rtype: Argument
        """
        return cls(name=a["NAME"], type=a["TYPE"])


class Function(QueryBase):
    """Class representing a Function configuration."""

    kind = "function"

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
        argument_list: List[Argument],
        return_type: str,
        language: Optional[str] = "sql",
        description: Optional[str] = None,
        static: Optional[bool] = True,
        destination_data_mart: Optional[str] = None,
    ):
        """Create a Function object.

        :param name: name of the function
        :type name: str
        :param query: SQL body of the function
        :type query: str
        :param version: version of the function
        :type version: str
        :param argument_list: the list of arguments of the function
        :type argument_list: List[Argument]
        :param return_type: the SQL return type of the function
        :type return_type: str
        :param language: the language of the function, defaults to sql
        :type language: Optional[str]
        :param description: description of the function, defaults to None
        :type description: Optional[str], optional
        """
        super().__init__(
            name=name,
            query=query,
            version=version,
            encrypt=False,
            static=static,
            destination_data_mart=destination_data_mart,
        )
        self.argument_list = argument_list
        self.description = description
        self.return_type = return_type
        self.language = language

    @classmethod
    def from_dict(cls, d: dict):
        """Create a View object form a dictionary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: View
        :rtype: View
        """
        function = cls(
            name=d.get("NAME", ""),
            query=d["QUERY"],
            version=d["VERSION"],
            description=d.get("DESCRIPTION"),
            static=d.get("STATIC", True),
            destination_data_mart=d.get("DESTINATION_DATA_MART"),
        )
        function.destination_table = d.get("DESTINATION_TABLE")
        function.dependencies = [
            Dependency.from_dict(x) for x in d.get("DEPENDS_ON") or []
        ]
        function.argument_list = [
            Argument.from_dict(a) for a in d.get("ARGUMENT_LIST") or []
        ]
        function.parsing_dependencies = [
            Dependency.from_dict(x) for x in d.get("PARSING_DEPENDS_ON") or []
        ]
        return function

    def to_dict(self) -> dict:
        """
        Serialize the View to a dictionary object.

        :return: the View as a dictionary object.
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
