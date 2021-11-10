=====
Functions
=====

Flycs lets you define BigQuery functions. These functions will be created in BigQuery during the CI/CD pipeline and usable in your transformations.

Example definition of a function in YAML:

.. code-block:: yaml

    QUERY: |
    LANGUAGE js AS '''
        if (name === null) {
        return null
        }
        name=name.split('&amp;').join('&');
        name=name.split('&#39;').join("'");
        name=name.split('&#233;').join("é");
        name=name.split('&#244;').join("ô");
        name=name.split('&#203;').join("Ë");
        name=name.split('&#239;').join("ï");
        name=name.split('&#201;').join("É");
        name=name.split('&#232;').join("è");
        name=name.split('&#226;').join("â");
        name=name.split('&#192;').join("À");
        name=name.split('&#238;').join("î");
        name=name.split('&#252;').join("ü");
        name=name.split('&#234;').join("ê");
        name=name.split('&#235;').join("ë");
        name=name.split('&#214;').join("Ö");
        name=name.split('&#231;').join("ç");
        name=name.split('&#246;').join("ö");
        name=name.split('&#200;').join("È");
        name=name.split('&quot;').join("'");
        name=name.split('&#199;').join("Ç");
        name=name.split('&#220;').join("Ü");
        name=name.split('&#224;').join("à");
        name=name.split('&#212;').join("Ô");
        name=name.split('&#228;').join("ä");
        return name;
    ''';

    VERSION: 1.0.0
    DESCRIPTION: "A function used to decode HTML entities"
    KIND: function
    LANGUAGE: javascript
    ARGUMENT_LIST:
    - NAME: name
      TYPE: STRING
    RETURN_TYPE: STRING


The functions definition must be places into a folder called `functions` in contrast with transformation that must be places into a `queries` folder.
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
        └── views
            └── data_warehouse
                └── view_simple_copy.yaml


Example definition of a view using python SDK:

.. code-block:: python

    from flycs_sdk.functions import Function, Argument

    my_view = Function(
        name="func_decode_html",
        query="""
        LANGUAGE js AS '''
            if (name === null) {
            return null
            }
            name=name.split('&amp;').join('&');
            name=name.split('&#39;').join("'");
            name=name.split('&#233;').join("é");
            name=name.split('&#244;').join("ô");
            name=name.split('&#203;').join("Ë");
            name=name.split('&#239;').join("ï");
            name=name.split('&#201;').join("É");
            name=name.split('&#232;').join("è");
            name=name.split('&#226;').join("â");
            name=name.split('&#192;').join("À");
            name=name.split('&#238;').join("î");
            name=name.split('&#252;').join("ü");
            name=name.split('&#234;').join("ê");
            name=name.split('&#235;').join("ë");
            name=name.split('&#214;').join("Ö");
            name=name.split('&#231;').join("ç");
            name=name.split('&#246;').join("ö");
            name=name.split('&#200;').join("È");
            name=name.split('&quot;').join("'");
            name=name.split('&#199;').join("Ç");
            name=name.split('&#220;').join("Ü");
            name=name.split('&#224;').join("à");
            name=name.split('&#212;').join("Ô");
            name=name.split('&#228;').join("ä");
            return name;""",
        version="1.0.0",
        description="A function used to decode HTML entities",
        argument_list=[Argument(name="name", type="STRING")],
        return_type="STRING",
        language="javascript",
        destination_data_mart=None, # only required when creating a function in a data_mart project,
    )
