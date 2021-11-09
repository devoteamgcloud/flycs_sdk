Changelog
=========

## v0.10.0 (2021-11-09)

### New - Dev|pkg|test|doc

* Add location field on Entity class. [Christophe de Carvalho]

  This fiels allow user to manually overwrite the default location of the
  associated dataset.

### Changes - Dev|pkg|test|doc

* Upgrade black formatter version. [Christophe de Carvalho]

* Do not give default value for PERSIST_BACKUP field. [Christophe de Carvalho]

  If the user does not specify a value, then the airflow-deployment code
  default behavior is used. So we should not enforce a default value here
  in the SDK cause it overwrite the default behavior in airflow-deployment


## v0.8.1 (2021-07-26)

### New - Dev|pkg|test|doc

* Add `destination_data_mart` field into query base class to make it available in View class. [Christophe de Carvalho]

* Add static field into query base class to make it available into View class. [Christophe de Carvalho]

* Add Kind field at entity level (#25) [Jordy Heusdens]

  * adding kind at entity level

  * fixed tests and added test for entity level kind

  * added documentation

  * merge fix

  * pre commit docstring fix

  * created entityKind + PipelineKind + adapted tests

### Changes - Dev|pkg|test|doc

* Drop tox to run CI pipeline. [Christophe de Carvalho]


## v0.8.0 (2021-07-26)

### New - Dev|pkg|test|doc

* Introduce QueryBase class. [Christophe de Carvalho]

  This class is the base for any other class that uses SQL query.
  It has 2 child classes, Transformation and View.

* Add concept of View object. [Christophe de Carvalho]

  You can now define views.
  The work mostly the same way Transformation do, but without all the
  tables related configurations.


## v0.7.2 (2021-05-18)

### New - Dev|pkg|test|doc

* Allow pipeline to have an empty start_time value. [Christophe de Carvalho]


## v0.6.0 (2021-02-25)

### New - Dev|pkg|test|doc

* Allow pipeline to trigger other pipeline. [Christophe de Carvalho]

  You can now specify a Pipeline object in the schedule field of another pipeline.
  When doing so, the pipeline will be triggered when the other one
  finishes.


v0.5.0 (2020-10-12)
-------------------

New - Dev|pkg|test|doc
~~~~~~~~~~~~~~~~~~~~~~
- Query parser with Zeta SQL that will detect query errors. [Charly-
  Fourcast]
- Lineage detection for every query. [Charly-Fourcast]
- Automatic AEAD BigQuery function injectioninto the queries for PII
  fields. [Charly-Fourcast]
