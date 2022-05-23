=====
Functions
=====

Flycs lets you define BigQuery functions. These functions will be created in BigQuery during the CI/CD pipeline and usable in your transformations.

.. note::
   Javascript functions using external code libraries are currently not supported.

   If you still want to use javascript UDF importing external packages in your Flycs
   transformation, you can create them in the raw dataset & reference it from this dataset.
   **(Works only when PII module is deactivated !)**

Example definition of a javascript UDF function called "func_decode_html" in YAML:

.. code-block:: yaml

    QUERY: |
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

    VERSION: 1.0.0
    DESCRIPTION: "A function used to decode HTML entities"
    KIND: function
    LANGUAGE: javascript
    ARGUMENT_LIST:
    - NAME: name
      TYPE: STRING
    RETURN_TYPE: STRING

Example definition of a SQL UDF function called "simple_mult_func" in YAML:

.. code-block:: yaml

    QUERY: |
      arg_IN * 2

    VERSION: 1.0.0
    DESCRIPTION: "A function used to multiply the input by 2"
    KIND: function
    ARGUMENT_LIST:
      - NAME: arg_IN
        TYPE: INTEGER
    RETURN_TYPE: INTEGER

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
        │   └── staging
                └── simple_mult_func.yaml
        │   └── data_warehouse
        │       └── func_decode_html.yaml
        └── views
            └── data_warehouse
                └── view_simple_copy.yaml


Example definition of the previous javascript function using python SDK:

.. code-block:: python

    from flycs_sdk.functions import Function, Argument

    my_function = Function(
        name="func_decode_html",
        query="""
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


Example of a transformation that use the function "simple_mult_func" defined in the staging stage :

.. code-block:: yaml

    QUERY: |
      SELECT val, self.staging.simple_mult_func(val) AS result
      FROM UNNEST([1,2,3,4]) AS val;

    VERSION: 1.0.0

    STATIC: true
