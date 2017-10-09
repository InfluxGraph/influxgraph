Docker Compose Stack
=====================

Provided here is docker compose configuration for quickly and easily setting up a Grafana, InfluxDB, Telegraf and InfluxGraph stack.

.. code-block:: shell

   pip install docker-compose

to install docker-compose.

Included Services
===================

* Grafana dashboard
* InfluxDB with Graphite ingestion service on port ``2003`` and ``measurement.field*`` default template
* Telegraf agent writing to InfluxDB via native influx line protocol
* InfluxGraph and Graphite API with built-in templates

Deploying
==========

The configuration presented here can be deployed as-is on any platform supporting docker compose like `AWS ECS <http://docs.aws.amazon.com/AmazonECS/latest/developerguide/cmd-ecs-cli-compose.html>`_, `Azure <https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-docker-compose>`_, `Google Cloud Platform <https://cloud.google.com/community/tutorials/docker-compose-on-container-optimized-os>`_, locally, on on-premise infrastructure et al.

Using Stack
============

Run

.. code-block:: shell

   docker-compose up

in this directory.

The result is a running Grafana instance at ``localhost:3000`` with Telegraf collected metrics available via InfluxGraph and the Graphite API.

This enables out of the box graphs like this:

.. image:: https://gist.githubusercontent.com/pkittenis/ead27ffee996a53ad66c50522cfff85f/raw/f8965b6081905be4fc1c021caec395f612b10679/monitoring_service_docker.png

Ingestion
----------

Graphite line protocol ingestion port is available at ``localhost:2003`` with template ``measurement.field*`` as defined in `compose file <https://github.com/InfluxGraph/influxgraph/blob/master/docker/compose/docker-compose.yml#L22>`_.

See `compose <https://github.com/InfluxGraph/influxgraph/blob/master/docker/compose/docker-compose.yml>`_ and `graphite-api.yaml <https://github.com/InfluxGraph/influxgraph/blob/master/docker/compose/graphite-api.yaml>`_ files in this directory for configuration details. InfluxDB and Grafana data volumes are exported from the docker host - data and dashboards are preserved.
