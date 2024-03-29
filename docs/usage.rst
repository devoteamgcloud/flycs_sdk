=====
Usage
=====

Getting started
###############

The simplest way to use the SDK is to define your pipeline, entities and transformations into the same python file:

.. literalinclude:: examples/basic.py
  :language: python

While this is very easy to use, usually you will want to put a bit more structure into the different types of object you create.
For example we could image to have a file per pipeline and one file per entity. This layout would look like:

::

  |── pipelines
  │   ├── entity.py
  │   ├── __init__.py
  │   └── pipeline.py


Let's go over the content of each file:

- **entity.py**: in this file we define one entity.

.. code-block:: python

  from flycs_sdk.entities import Entity

  stage_config = {
      "raw": {"table_1": "1.0.0", "table_2": "1.0.0"},
      "staging": {"table_3": "1.0.0", "table_4": "1.0.0"},
      "data_warehouse": {"table_5": "1.1.0"},
  }
  entity = Entity("entity", "1.0.0", stage_config)

- **pipeline.py**: In this file we define one pipeline and import the entity defined in the *entity.py* file

.. code-block:: python

  from datetime import datetime, timezone
  from flycs_sdk.pipelines import Pipeline, PipelineKind

  from .entity import entity

  my_pipeline = Pipeline(
      name="my_pipeline",
      version="1.0.0",
      schedule="* 12 * * *",  # this is using cron notation
      entities=[entity],
      kind=PipelineKind.VANILLA,
      start_time=datetime.now(tz=timezone.utc),
  )

- **__init__.py**: In this file, we create the *pipelines* list and import the pipelines define in *pipeline.py*.

.. code-block:: python

  from .pipeline import my_pipeline
  pipelines = [my_pipeline]


How to use parametrized pipeline to keep the code DRY ?
#######################################################

It can happens that you end up with a lot of pipelines that looks nearly exactly the same.
To avoid this, the SDK offers the *ParametrizePipeline* and *ParametrizeEntity* class. With it, you can pass some parameters to your pipeline. The SDK would then generate automatically a new
pipeline for each possible combination of each parameters.

A set of parameters looks like this:

.. code-block:: python

    pipeline_parameters = {
        "language": ["nl", "fr"],
        "country": ["be", "en"],
    }

Such a parameters would generate 4 pipelines, one for each possible combination of parameter:

.. code-block:: python

    {"language": "nl", "country": "be"},
    {"language": "nl", "country": "en"},
    {"language": "fr", "country": "be"},
    {"language": "fr", "country": "en"},



Parameterized pipeline and entity also allow to introduce custom logic. Here is an example how to use it:

.. literalinclude:: examples/parametrize_pipeline.py
  :language: python
