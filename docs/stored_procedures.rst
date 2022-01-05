=====
Stored Procedures
=====

Flycs lets you define BigQuery stored procedures. These stored procedures will be created in BigQuery during the CI/CD pipeline and usable in your transformations.

Example definition of a stored procedure in YAML:
(Note that the argument mode must be specified IN, OUT or INOUT)

.. code-block:: yaml

    QUERY: |
      BEGIN
          SET argOUT = argIN + argOUT ;
      END
    VERSION: 1.0.0
    DESCRIPTION: "this is a sum stored procedure"
    KIND: stored_procedure
    ARGUMENT_LIST:
        - NAME: argIN
          TYPE: INTEGER
          MODE: IN
        - NAME: argOUT
          TYPE: INTEGER
          MODE: OUT


The stored procedures definition must be places into a folder called `procedures` in contrast with transformation that must be places into a `queries` folder.
Here is an example tree from a flycs repository:


.. code-block:: shell

    bigquery
    └── demo
        ├── queries
        │   ├── datalake
        │   │   ├── history.yaml
        │   │   └── simple_copy.yaml
        │   ├── data_mart
        │   │   ├── salary.yaml
        │   │   └── simple_copy.yaml
        │   ├── data_warehouse
        │   │   ├── manipulating_pii_fields.yaml
        │   │   └── simple_copy.yaml
        │   └── staging
        │       ├── simple_copy.yaml
        │       └── time_test.yaml
        └── functions
        │   └── data_warehouse
        │       └── func_decode_html.yaml
        └── procedures
        │   └── staging
        │       └── simple_sum.yaml
        └── views
            └── data_warehouse
                └── view_simple_copy.yaml


Example definition of a view using python SDK:

.. code-block:: python

    from flycs_sdk.procedures import StoredProcedure, Argument

    my_procedure = StoredProcedure(
        name="simple_sum",
        query="""
            BEGIN
            SET argOUT = argIN + argOUT ;
            END""",
        version="1.0.0",
        description="this is a sum stored procedure",
        argument_list=[Argument(name="argIN", type="INTEGER", mode="IN"), Argument(name="argOUT", type="INTEGER", mode="OUT")],
        destination_data_mart=None, # only required when creating a stored procedure in a data_mart project,
    )
