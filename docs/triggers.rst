=================
Pipeline triggers
=================

Flycs let you configure some triggers for your pipeline. A Trigger allow your pipeline to be triggered by an external event.
The supported type of triggers are:

- `PubSub topic`
- `Google Cloud Storage`
- `Other pipeline`
- `Multiple Other pipelines`

`PubSub topic`
############

This triggers creates a subscription to the topic and then waits for any message to come. One a message is received, the rest of the pipeline is executed.

The PubSub trigger has 2 properties you can configure:

- **topic**: The full path to a pubsub topic. The topic MUST exist before the pipeline with the trigger is executed.
- **subscription_project**: Optional property that let you choose in which project the subscription to the topic will be created.

Here is an example Pipeline definition using the PubSub trigger:

.. code-block:: yaml

    name: demo
    kind: vanilla
    version: 1.0.0
    entities:
    - name: demo
        version: 1.0.0
        stage_config:
        - name: preamble
            versions: {}
        - name: staging
            versions:
            simple_copy: "1.0.0"
        - name: datalake
            versions:
            simple_copy: "1.0.0"
            history: "1.0.0"
        - name: data_warehouse
            versions:
            simple_copy: "1.0.0"
            manipulating_pii_fields: "1.0.0"
        - name: data_mart
            versions:
            simple_copy: "1.0.0"
            salary: "1.0.0"
    start_time: "2021-01-01T00:00:00"
    trigger:
        type: "pubsub"
        topic: "projects/my_project/topics/my_topic"
        subscription_project: my_other_project

The same pipeline defined using the Flycs SDK:

.. code-block:: python

    from datetime import datetime, timezone

    from flycs_sdk.pipelines import Pipeline, PipelineKind
    from flycs_sdk.triggers import PubSubTrigger
    from .entities import entity # entities are define in another modules, we just import them here.

    p = Pipeline(
        name="demo",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=datetime.now(tz=timezone.utc),
        trigger=PubSubTrigger(topic="projects/my_project/topics/my_topic", subscription_project="my_other_project"),
    )

Note how the **schedule** property is not required on the pipeline definition when you specify a trigger.
The reason for this is that Flycs will automatically configure your pipeline to be schedule every seconds so that it is always running and waiting on the trigger event to occur.


`Google Cloud Storage`
######################

The event sent from GCS can actually be of 3 types:

- **Trigger when an object exists**: As soon as the watched object is created and for as long as the object exists, the DAG is triggered.
- **Trigger when an update change**: When the update time of an object change, the DAG is triggered.
- **Trigger by watching a prefix in a bucket**: As soon as the prefix or any object under this prefix exists, the DAG is triggered.

Here are the YAML example how to define these GSC triggers. For brevity , only the `trigger` block is shown here.

Object exist trigger:

.. code-block:: yaml

    trigger:
        type: "gcs_object_exist"
        bucket: "gcs-trigger"
        object: "subdir/my_object"

.. code-block:: python

    GCSObjectExistTrigger(
        bucket="gcs-trigger",
        object="subdir/my_object"
    )

Object update trigger:

.. code-block:: yaml

    trigger:
        type: "gcs_object_change"
        bucket: "gcs-trigger"
        object: "subdir/my_object"

.. code-block:: python

    GCSObjectChangeTrigger(
        bucket="gcs-trigger",
        object="subdir/my_object"
    )

Prefix watch trigger:

.. code-block:: yaml

    trigger:
        type: "gcs_watch_prefix"
        bucket: "gcs-trigger"
        prefix: "my_prefix"

.. code-block:: python

    GCSPrefixWatchTrigger(
        bucket="gcs-trigger",
        prefix="my_prefix"
    )



`Other pipeline`
################

This type of trigger is a bit different from the other one because it does not involve an external event. Instead, the pipeline is triggered whenever another pipeline is done.

The way to configure this trigger is also a bit different, here is an example. Here we define 2 pipelines called master and child. Master is responsible to trigger child.

.. code-block:: yaml

    # Pipeline master
    name: master
    kind: vanilla
    version: 1.0.0
    entities:
        ... # removed for brevity
    schedule: "10 10 * * *"
    start_time: "2021-01-01T00:00:00"

    # Pipeline child
    name: child
    kind: vanilla
    version: 1.0.0
    entities:
        ... # removed for brevity
    schedule: "master_1.0.0" # this is where the magic happens, by specifying the name + version of another pipeline, this pipeline will be automatically triggered.
    start_time: "2021-01-01T00:00:00"


Same example with the python SDK:


.. code-block:: python

    master = Pipeline(
        name="master",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=datetime.now(tz=timezone.utc),
        schedule="10 10 * * *",
    )

    child = Pipeline(
        name="child",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=datetime.now(tz=timezone.utc)
        schedule=master, # Here we pass the master Pipeline object directly into the `schedule` field.
    )

`Multiple Other pipelines`
################

This trigger is used when you want a child pipeline to be triggered by multiple parents pipelines. Using this trigger will actually used sensor on the child pipeline to wait for all the parents pipelines to finish.

This feature use ExternalSensor on the child pipeline. The schedule period of it will be automatically computed to run at the wider cron job scheduled of the list of the parents pipelines.

E.g : Having pipeline_A running 30 9 * * * (every day UTC at 9.30) and pipeline_B running \*/30 * * * * (every 30 minutes) with a child pipeline_C depending on both A and B. Then, the pipeline_C will be scheduled with the cron configuration 30 9 * * \*.

Using yaml definition :

.. code-block:: yaml

    # parent pipeline_A
    name: pipeline_A
    kind: vanilla
    version: 1.0.0
    entities:
        ... # removed for brevity
    schedule: "30 9 * * *"
    start_time: "2021-01-01T00:00:00"

    # parent pipeline_B
    name: pipeline_B
    kind: vanilla
    version: 1.0.0
    entities:
        ... # removed for brevity
    schedule: "*/30 * * * *"
    start_time: "2021-01-01T00:00:00"

    # child pipeline_C
    name: pipeline_C
    kind: vanilla
    version: 1.0.0
    entities:
        ... # removed for brevity
    schedule: ["pipeline_A","pipeline_B"]
    start_time: "2021-01-01T00:00:00" # /!\ start_time of all the parents pipeline (A & B) must be the same as the pipeline_C


Using python pdk :


.. code-block:: python

    now_datetime = datetime.now(tz=timezone.utc)

    pipeline_A = Pipeline(
        name="pipeline_A",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=now_datetime,
        schedule="30 9 * * *",
    )

    pipeline_B = Pipeline(
        name="pipeline_B",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=now_datetime,
        schedule="*/30 * * * *",
    )

    pipeline_C = Pipeline(
        name="pipeline_C",
        version="1.0.0",
        entities=[entity],
        kind=PipelineKind.VANILLA,
        start_time=now_datetime,
        schedule=[pipeline_A, pipeline_B], # Here we pass the list of parents pipelines objects.
    )
