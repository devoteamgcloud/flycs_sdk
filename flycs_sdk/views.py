"""Module containing view classes."""

from typing import Optional


class View:
    """Class representing a View configuration."""

    kind = "view"

    def __init__(
        self, name: str, query: str, version: str, description: Optional[str] = None,
    ):
        """Create a View object.

        :param name: name of the view
        :type name: str
        :param query: SQL query of the view
        :type query: str
        :param version: version of the view
        :type version: str
        :param description: description of the view, defaults to None
        :type description: Optional[str], optional
        """
        self.name = name
        self.query = query
        self.version = version
        self.description = description
        self.destination_table = None
        self.dependencies = []
        self.parsing_dependencies = []

    @classmethod
    def from_dict(cls, d: dict):
        """Create a View object form a dictionnary created with the to_dict method.

        :param d: source dictionary
        :type d: dict
        :return: View
        :rtype: View
        """
        view = cls(
            name=d.get("NAME", ""),
            query=d["QUERY"],
            version=d["VERSION"],
            description=d.get("DESCRIPTION"),
        )
        view.destination_table = d.get("DESTINATION_TABLE")
        return view

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
        )
