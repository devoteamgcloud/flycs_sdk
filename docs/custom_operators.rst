================
Custom operators
================

The custom operators let you insert your own code into the generated Airflow DAGs.
This is useful is you need to use other Airflow operator then BigQuery in your DAGs.

The way you define custom code is by defining a function which return an instance of any Airflow Operator.
Then you link this function to a CustomCode object.

.. literalinclude:: examples/custom_operator.py
  :language: python
