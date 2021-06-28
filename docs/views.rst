=====
Views
=====

Flycs lets you define BigQuery views. While it was already possible to define views by hacking a Transformation with an output set to false and manually
writing the view creating in SQL, Flycs now support view as a native object in the SDK.

Example definition of a view in YAML:

.. code-block:: yaml

    QUERY: |
        SELECT * FROM self.data_warehouse.simple_copy
    VERSION: "1.0.0"
    KIND: view
    DESCRIPTION: |
        this is a view on the simple_copy table, modified

The views must be places into a folder called `views` in contrast with transformation that must be places into a `queries` folder.
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
        └── views
            └── data_warehouse
                └── view_simple_copy.yaml


Example definition of a view using python SDK:

.. code-block:: python

    from flycs_sdk.views import View

    my_view = View(
        name="my_view",
        query="SELECT * FROM self.data_warehouse.simple_copy",
        version="1.0.0",
        description="this is a view on the simple_copy table, modified"
    )
