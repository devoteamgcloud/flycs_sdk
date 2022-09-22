================
Custom operators
================

The custom operators let you insert your own code into the generated Airflow DAGs.
This is useful is you need to use other Airflow operator then BigQuery in your DAGs.

The way you define custom code is by defining a function which return an instance of any Airflow Operator.
Then you link this function to a CustomCode object.

.. literalinclude:: examples/custom_operator.py
  :language: python

How to use Custom arguments inside builder function?
#######################################################

It is possible to use kwargs dict arguments inside the builder function of the Custom Operator.

This feature allows you to create specific airflow Operators based on arguments value or the environment the DAG is running.
To be able to access those argument, you must defined them using the func_kwargs attribute when creatin the CustomCode object.

See the following example :

.. code-block:: python

    def build(dag, env=None, task_type = "msg", **kwargs):
        from airflow.operators.dummy import DummyOperator
        if kwargs.get("key"):
            return DummyOperator(task_id=f"dummy_{env}_{task_type}_{kwargs['key']}",dag=dag)
        else :
            return DummyOperator(task_id=f'dummy_{env}_{task_type}',dag=dag)


    mycode = CustomCode(
        name="dummy_env_custom",
        version="1.0.0",
        operator_builder=build,
        requirements=[],
        func_kwargs={"task_type": "msg"},
    )
    mycode_kwargs = CustomCode(
        name="dummy_env_custom_kwargs",
        version="1.0.0",
        operator_builder=build,
        requirements=[],
        func_kwargs={"task_type": "msg","key":"random_string"},
    )

Two CustomCode objects are created in this example. The first one defines only one argument (task_type) the result of the builder function will render a DummyOperator named based on the environment the DAG is running and based on the task_type argument value.

The second CustomCode object, add the argument key with the value msg. Because this second argument is not already defined in the builder function, you must include the kwargs dictionnary argument in the builder function and access the key argument via the kwargs.get(“key”) function.
