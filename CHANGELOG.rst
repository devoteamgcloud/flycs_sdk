Changelog
=========

v0.7.2 (2021-05-18)
-------------------

New - Dev|pkg|test|doc
~~~~~~~~~~~~~~~~~~~~~~
- Allow pipeline to have an empty start_time value. [Christophe de
  Carvalho]


v0.6.0 (2021-02-25)
-------------------

New - Dev|pkg|test|doc
~~~~~~~~~~~~~~~~~~~~~~
- Allow pipeline to trigger other pipeline. [Christophe de Carvalho]

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
