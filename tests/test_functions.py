"""Test functions module."""
from typing import List

import pytest
from flycs_sdk.functions import Function, Argument

function_name = "my_function"
function_version = "1.0.0"
function_query = "SELECT * FROM TABLE;"
function_description = "this is my function"
function_return_type = "STRING"
function_language = "sql"

argument_name = "my_argument"
argument_type = "STRING"

other_argument_name = "other_argument"
other_argument_type = "INT64"


class TestArgument:
    @pytest.fixture
    def my_argument(self) -> Argument:
        return Argument(name=argument_name, type=argument_type)

    def test_init(self, my_argument: Argument):
        assert my_argument.name == argument_name
        assert my_argument.type == argument_type

    def test_to_dict(self, my_argument: Argument):
        assert my_argument.to_dict() == {"NAME": argument_name, "TYPE": argument_type}

    def test_serialize_deserialize(self, my_argument):
        d = my_argument.to_dict()
        argument2 = my_argument.from_dict(d)
        assert my_argument == argument2


class TestFunction:
    @pytest.fixture
    def my_arguments(self) -> List[Argument]:
        return [
            Argument(name=argument_name, type=argument_type),
            Argument(name=other_argument_name, type=other_argument_type),
        ]

    @pytest.fixture
    def my_function(self, my_arguments: List[Argument]) -> Function:
        return Function(
            name=function_name,
            query=function_query,
            version=function_version,
            description=function_description,
            argument_list=my_arguments,
            return_type=function_return_type,
            language=function_language,
        )

    def test_init(self, my_function: Function, my_arguments: List[Argument]):
        assert my_function.name == function_name
        assert my_function.query == function_query
        assert my_function.version == function_version
        assert my_function.description == function_description
        assert my_function.argument_list == my_arguments
        assert my_function.return_type == function_return_type

    def test_to_dict(self, my_function: Function):
        assert my_function.to_dict() == {
            "NAME": function_name,
            "QUERY": function_query,
            "VERSION": function_version,
            "DESCRIPTION": function_description,
            "DESTINATION_TABLE": None,
            "KIND": "function",
            "STATIC": True,
            "DESTINATION_DATA_MART": None,
            "DEPENDS_ON": [],
            "PARSING_DEPENDS_ON": [],
            "ARGUMENT_LIST": [
                {"NAME": "my_argument", "TYPE": "STRING"},
                {"NAME": "other_argument", "TYPE": "INT64"},
            ],
            "LANGUAGE": function_language,
            "RETURN_TYPE": function_return_type,
        }

    def test_serialize_deserialize(self, my_function):
        d = my_function.to_dict()
        function2 = my_function.from_dict(d)
        assert my_function == function2
