from datetime import datetime, timezone

from flycs_sdk.entities import ParametrizedEntity
from flycs_sdk.pipelines import ParametrizedPipeline, PipelineKind

# To leverage the power of the ParametrizedPipeline, create a new class that inherits the ParametrizedPipeline class


class MyPipeline(ParametrizedPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # You can overwrite the `schedule` and `start_time` property to introduce custom logic
    # in your pipeline
    # Here we return a different schedule time in the case the value of the parameters `language` is equal to `fr`
    @property
    def schedule(self):
        if self.parameters["language"] == "fr":
            return "* 1 * * *"
        else:
            return "* 12 * * *"


# It is also possible to customize the behavior of the entities.
# To do so, create a class that inherits of the ParametrizedEntity class and overwrite the `get_stage_versions` method.


class MyEntity(ParametrizedEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # here we introduce a custom behavior when the state is "staging" and the value of the parameters `language` is equal to `fr`
    def get_stage_versions(self, stage, parameters):
        if "stage" == "staging" and parameters["language"] == "fr":
            return {"table_3": "1.1.0", "table_4": "2.0.0"}
        else:
            return self.stage_config[stage]


# Once you have your new classes defined, you can create the objects normally
stage_config = {
    "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
    "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
    "data_warehouse": {"table_5": "1.1.0"},
}
entity1 = MyEntity("entity1", "1.0.0", stage_config)

# Once the entities are defined, we can create pipelines.
p1 = MyPipeline(
    name="my_pipeline",
    version="1.0.0",
    schedule="* 12 * * *",
    entities=[entity1],
    kind=PipelineKind.VANILLA,
    start_time=datetime.now(tz=timezone.utc),
    parameters={
        "language": ["fr", "en"],
        "country": {"be", "nl"},
    },  # this is an extra argument compare to the normal Pipeline class
)

# make the pipelines available to be discovered by the rest of the Flycs ecosystem
pipelines = [p1]
