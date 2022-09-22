=====
Transformations
=====

Flycs lets you define BigQuery queries that will be run as BigQuery Operators during the DAG run in which the query is used.
To define a query using the python SDK you must use a Transformation object.

.. code-block:: python

    from flycs_sdk.transformations import Transformation
    query = Transformation(
        name="simple_copy",
        query="SELECT * FROM raw.alpha.employees.employees AS raw",
        version="1.0.0",
        static=True,
    )

How to use the Flycs SDK to create parameterized queries ?
#######################################################

It can happen that you need to generate some queries with information that are dynamic, or you want to create a pipeline that uses the same query but on different table.
To do all of these things, the easiest way is to leverage the power of the Flycs SDK.

In the following example we will show how you can define a pipeline that contains parameterized queries.

.. code-block:: python

    from datetime import datetime, timezone

    from flycs_sdk.pipelines import Pipeline, PipelineKind
    from flycs_sdk.entities import Entity
    from flycs_sdk.transformations import Transformation

    # define your list of parameters
    parameters = [
        ("table1", "tables2"),
        ("table3", "tables4"),
        ("table5", "tables6"),
    ]

    # generate the transformation for each parameter
    transformations = []
    for table1, table2 in parameters:
        query = Transformation(
            name="transformation_" + "_".join([table1, table2]),
             # Notice how we generate the content of the query using
             # the parameters define at the top of the file
            query=f"SELECT * FROM {table1} LEFT JOIN {table2}",
            version="1.0.0",
        )
        transformations.append(query)

    # define the entity
    entity = Entity(
        name="my_entity",
        version="1.0.0",
        stage_config={},
        transformations={},
    )
    # insert the transformations into the entity
    for t in transformations:
        # stage_config is a dict that contains the stage name as key and a dictionnary of
        # transformation name and version as value
        entity.stage_config["staging"] = {t.name: t.version for t in transformations}
        # stage_config is a dict that contains the stage name as key and a dictionnary of
        # transformation name and transformation object as value
        entity.transformations["staging"] = {t.name: t for t in transformations}

    # define the pipeline
    my_pipeline = Pipeline(
        name="my_pipeline",
        version="1.0.0",
        schedule="10 10 * * *",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=datetime.now(tz=timezone.utc),
    )

    # to be picked up by the framework, all the pipelines needs to be
    # added to a variables called `pipelines`
    pipelines = [my_pipeline]
