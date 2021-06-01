from datetime import datetime, timezone

from flycs_sdk.entities import Entity
from flycs_sdk.pipelines import Pipeline, PipelineKind
from flycs_sdk.transformations import Transformation

# Define your transformation SQL query using jinja template for the table name and define the list of table on which this transformation should be applied
query = Transformation(
    name="my_query",
    query="SELECT * FROM {table_name}",
    version="1.0.0",
    tables=["tables1", "tables2"],
)

# Then define your entity and pipeline as usual
stage_config = {
    "raw": {"my_query": "1.0.0"},
    "staging": {"my_query": "1.0.0"},
}
entity1 = Entity("entity1", "1.0.0", stage_config)
entity1.transformations = {
    "raw": {"my_query": query},
    "staging": {"my_query": query},
}

p1 = Pipeline(
    name="my_pipeline",
    version="1.0.0",
    schedule="* 12 * * *",
    entities=[entity1],
    kind=PipelineKind.VANILLA,
    start_time=datetime.now(tz=timezone.utc),
)


# expose the pipeline to the module as usual
pipelines = [p1]
