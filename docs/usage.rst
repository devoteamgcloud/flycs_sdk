=====
Usage
=====

The first step is to define entities using the Flycs SDK

.. code-block:: python

    from flycs_sdk.entities import Entity

    stage_config = {
        "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
        "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
        "data_warehouse": {"table_5": "1.1.0"},
    }
    entity = Entity(entity_name, entity_version, stage_config)

Once the entities are defined, we can create pipelines.
To be able to be discovered, the pipelines needs to be aggregated into a list called 'pipelines' located at the root of the module.

.. code-block:: python

    from flycs_sdk.pipelines import Pipeline, PipelineKind

    # import entities defined in another module
    from .entities import entities

    p1 = Pipeline(
        name="my_pipeline",
        version="1.0.0",
        schedule="* 12 * * *", # this is using cron notation
        entities=entities,
        kind=PipelineKind.VANILLA,
        start_time=1607507618, # this is a UNIX timestamp
    )

    # make the pipelines available to be discovered by the rest of the Flycs ecosystem
    pipelines = [p1]
