from datetime import datetime, timezone

from flycs_sdk.custom_code import CustomCode, Dependency
from flycs_sdk.entities import Entity
from flycs_sdk.pipelines import Pipeline, PipelineKind
from flycs_sdk.transformations import Transformation

ENTITY_NAME = "my_dataset"

transformations = []
query = Transformation(
    name="simple_copy",
    query="SELECT * FROM raw.alpha.employees.employees AS raw",
    version="1.0.0",
    static=True,
)
transformations.append(query)


def build(dag, env=None):
    from airflow.operators.dummy_operator import DummyOperator

    return DummyOperator(dag=dag, task_id="custom_code")


mycode = CustomCode(
    name="my_custom_code",
    version="1.0.0",
    operator_builder=build,
    dependencies=[
        Dependency(ENTITY_NAME, "staging", query.name)
    ],  # use the dependencies argument to place the airflow operator at the right place in your DAG
    requirements=[
        "airflow==1.10.0",
    ],  # requirements let you define dependencies required by the build function
)


# define the entity
entity = Entity(
    name=ENTITY_NAME,
    version="1.0.0",
    stage_config={"staging": {t.name: t.version for t in transformations}},
    custom_operators={"staging": [mycode]},
)
# insert the transformations into the entity
for t in transformations:
    entity.transformations["staging"] = {t.name: t for t in transformations}


python_pipeline = Pipeline(
    name="python_pipeline",
    version="1.0.0",
    schedule="10 10 * * *",
    entities=[entity],
    kind=PipelineKind.VANILLA,
    start_time=datetime.now(tz=timezone.utc),
)
