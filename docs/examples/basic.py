from datetime import datetime, timezone

from flycs_sdk.entities import Entity
from flycs_sdk.pipelines import Pipeline, PipelineKind

# The first step is to define entities using the Flycs SDK
stage_config = {
    "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
    "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
    "data_warehouse": {"table_5": "1.1.0"},
}
entity1 = Entity("entity1", "1.0.0", stage_config)

# Once the entities are defined, we can create pipelines.
p1 = Pipeline(
    name="my_pipeline",
    version="1.0.0",
    schedule="* 12 * * *",  # this is using cron notation
    entities=[entity1],
    kind=PipelineKind.VANILLA,
    start_time=datetime.now(tz=timezone.utc),
)

# To be able to be discovered, the pipelines needs to be aggregated into a list called 'pipelines' located at the root of the module.
# make the pipelines available to be discovered by the rest of the Flycs ecosystem
pipelines = [p1]
