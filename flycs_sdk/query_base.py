"""This module contains base class for any "query" object (Transformation, views,...)."""

from abc import ABC, abstractmethod
from typing import Optional


class QueryBase(ABC):
    """Base class for any query based object."""

    def __init__(
        self,
        name: str,
        query: str,
        version: str,
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
