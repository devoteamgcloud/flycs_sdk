=================
Pipeline triggers
=================

Flycs let you configure some triggers for your pipeline. A Trigger allow your pipeline to be triggered by an external event.
The supported type of triggers are:

- `PubSub topic`_

PubSub topic
############

This triggers creates a subscription to the topic and then waits for any message to come. One a message is received, the rest of the pipeline is executed.

The PubSub trigger has 2 property you can configure:

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
