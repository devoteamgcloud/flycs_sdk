import pytest

from flycs_sdk.custom_code import CustomCode, Dependency, WrongSignatureError


class TestCustomCode:
    def test_imported_modules(self):
        def build(dag, env=None):
            from airflow.operators.dummy_operator import DummyOperator
            import os

            return DummyOperator(dag=dag)

        cc = CustomCode("mycode", "1.0.0", build, requirements=["airflow==1.10.0"])
        assert cc.requirements == ["airflow==1.10.0"]

        with pytest.raises(ValueError):
            CustomCode(
                "mycode", "1.0.0", build, requirements=["not a valid requirements"]
            )

    def test_validation_signature_builder(self):
        def build():
            from airflow.operators.dummy_operator import DummyOperator

            return DummyOperator()

        with pytest.raises(WrongSignatureError):
            cc = CustomCode("mycode", "1.0.0", build)

        def build(dag, env=None):
            from airflow.operators.dummy_operator import DummyOperator

            return DummyOperator()

        cc = CustomCode("mycode", "1.0.0", build)


class TestDependency:
    @pytest.fixture
    def dependency(self) -> Dependency:
        return Dependency("entity", "stage", "name")

    def test_to_dict(self, dependency):
        assert dependency.to_dict() == {
            "ENTITY": "entity",
            "STAGE": "stage",
            "NAME": "name",
        }

    def test_from_dict(self, dependency):
        loaded = Dependency.from_dict(dependency.to_dict())
        assert loaded == dependency
