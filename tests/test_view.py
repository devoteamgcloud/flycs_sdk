"""Test views module."""

import pytest
from flycs_sdk.views import View

view_name = "my_view"
view_version = "1.0.0"
view_query = "SELECT * FROM TABLE;"
view_description = "this is my view"


class TestView:
    @pytest.fixture
    def my_view(self) -> View:
        return View(
            name=view_name,
            query=view_query,
            version=view_version,
            description=view_description,
        )

    def test_init(self, my_view: View):
        assert my_view.name == view_name
        assert my_view.query == view_query
        assert my_view.version == view_version
        assert my_view.description == view_description

    def test_to_dict(self, my_view: View):
        assert my_view.to_dict() == {
            "NAME": view_name,
            "QUERY": view_query,
            "VERSION": view_version,
            "DESCRIPTION": view_description,
            "DESTINATION_TABLE": None,
            "KIND": "view",
            "ENCRYPT": None,
            "STATIC": True,
            "DESTINATION_DATA_MART": None,
            "DEPENDS_ON": [],
            "PARSING_DEPENDS_ON": [],
        }

    def test_serialize_deserialize(self, my_view):
        d = my_view.to_dict()
        view2 = my_view.from_dict(d)
        assert my_view == view2
