InfluxGraph
=================

An `InfluxDB`_ storage plugin for `Graphite-API`_. Graphite with InfluxDB data store from any kind of schema(s) used in the DB.

.. image:: https://img.shields.io/pypi/v/influxgraph.svg
  :target: https://pypi.python.org/pypi/influxgraph
  :alt: Latest Version
.. image:: https://travis-ci.org/InfluxGraph/influxgraph.svg?branch=master
  :target: https://travis-ci.org/InfluxGraph/influxgraph
  :alt: CI status
.. image:: https://coveralls.io/repos/InfluxGraph/influxgraph/badge.png?branch=master
  :target: https://coveralls.io/r/InfluxGraph/influxgraph?branch=master
  :alt: Coverage
.. image:: https://readthedocs.org/projects/influxgraph/badge/?version=latest
  :target: http://influxgraph.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status
.. image:: https://img.shields.io/pypi/wheel/influxgraph.svg
   :target: https://pypi.python.org/pypi/influxgraph
.. image:: https://img.shields.io/pypi/pyversions/influxgraph.svg
   :target: https://pypi.python.org/pypi/influxgraph


This project started as a re-write of `graphite influxdb <https://github.com/vimeo/graphite-influxdb>`_, now a separate project.


Installation
=============

Docker Compose
---------------

In `compose directory <https://github.com/InfluxGraph/influxgraph/tree/master/docker/compose>`_ can be found docker-compose configuration that will spawn all necessary services for a complete monitoring solution with:

* InfluxDB
* Telegraf
* Graphite API with InfluxGraph
* Grafana dashboard

To use, within compose directory run:

.. code-block:: shell

   docker-compose up

Grafana will be running on ``http://localhost:3000`` with Graphite datasource for InfluxDB data available at ``http://localhost``. Add a new Graphite datasource to Grafana - default Grafana user/pass is admin/admin - to create dashboards with.

See `compose configuration readme <https://github.com/InfluxGraph/influxgraph/tree/master/docker/compose>`_ for more details.

Docker Image
-------------

.. code-block:: shell

  docker pull ikuosu/influxgraph
  docker create  --name=influxgraph -p 8000:80 ikuosu/influxgraph
  docker start influxgraph

There will now be a Graphite-API running on ``localhost:8000`` from the container with a default InfluxDB configuration and memcache enabled. Finder expects InfluxDB to be running on ``localhost:8086`` by default.

The image will use a supplied ``graphite-api.yaml`` on build, when ``docker build`` is called on an InfluxGraph image.

`Docker file <https://github.com/InfluxGraph/influxgraph/blob/master/docker/Dockerfile>`_ used to build container can be found under ``docker`` directory of the repository.

.. note::

  If having issues with the container accessing the host's InfluxDB service then either use ``--network="host"`` when launching the container or build a new image with a provided configuration file containing the correct `InfluxDB host:port <https://github.com/InfluxGraph/influxgraph/blob/master/docker/graphite-api.yaml#L4>`_ destination.

Manual Installation
---------------------

.. code-block:: shell

  pip install influxgraph

Use of a local `memcached` service is highly recommended - see configuration section on how to enable.

Mimimal configuration for Graphite-API is below. See `Full Configuration Example`_ for all possible configuration options.

``/etc/graphite-api.yaml``

.. code-block:: yaml

  finders:
    - influxgraph.InfluxDBFinder

See the `Wiki <https://github.com/InfluxGraph/influxgraph/wiki>`_ and `Configuration`_ section for details.

.. contents:: Table of Contents

Main features
==============

* InfluxDB Graphite template support - expose InfluxDB tagged data as Graphite metrics with configurable metric paths
* Dynamically calculated group by intervals based on query date/time range - fast queries regardless of the date/time they span
* Configurable per-query aggregation functions by regular expression pattern
* Configurable per-query retention policies by query date/time range. Automatically use pre-calculated downsampled data in a retention policy for historical data
* Fast in-memory index for Graphite metric path queries as a Python native code extension
* Multi-fetch enabled - fetch data for multiple metrics with one query to InfluxDB
* Memcached integration
* Python 3 and PyPy compatibility
* Good performance even with extremely large number of metrics in the DB - generated queries are guaranteed to have ``O(1)`` performance characteristics

Google User's Group
=====================

There is a `Google user's group for discussion <https://groups.google.com/forum/#!forum/influxgraph>`_ which is open to the public.

Goals
======

* InfluxDB as a drop-in replacement data store to the Graphite query API
* Backwards compatibility with existing Graphite API clients like Grafana and Graphite installations migrated to InfluxDB data stores using Graphite input service *with or without* Graphite template configuration
* Expose native InfluxDB line protocol ingested data via the Graphite API
* Clean, readable code with complete documentation for public endpoints
* Complete code coverage with both unit and integration testing. Code has `>90%` test coverage and is integration tested against a real InfluxDB service
* Good performance at large scale. InfluxGraph is used in production with good performance on InfluxDB nodes with cardinality exceeding 5M and a write rate of over 5M metrics/minute or 66K/second.

The first three goals provide both

- A backwards compatible migration path for existing Graphite installations to use InfluxDB as a drop-in storage back-end replacement with no API client side changes required, meaning existing Grafana or other dashboards continue to work as-is.
- A way for native InfluxDB collection agents to expose their data via the *Graphite API* which allows the use of any Graphite API talking tool, the plethora of Graphite API functions, custom functions, functions across series, multi-series plotting and functions via Graphite glob expressions et al.

As of this time of writing, no alternatives exist with similar functionality, performance and compatibility.

Non-Goals
==========

* Graphite-Web support from the official Graphite project

Dependencies
=============

With the exception of `InfluxDB`_ itself, the other dependencies are installed automatically by ``pip``.

* ``influxdb`` Python module
* `Graphite-API`_
* ``python-memcached`` Python module
* `InfluxDB`_ service, versions ``1.0`` or higher

InfluxDB Graphite metric templates
==================================

`InfluxGraph` can make use of any InfluxDB data and expose them as Graphite API metrics, as well as make use of Graphite metrics added to InfluxDB as-is sans tags.

Even data written to InfluxDB by native InfluxDB API clients can be exposed as Graphite metrics, allowing transparent to clients use of the Graphite API with InfluxDB acting as its storage back-end.

To make use of tagged InfluxDB data, the finder needs to know how to generate a Graphite metric path from the tags used by InfluxDB.

The easiest way to do this is to use the Graphite service in InfluxDB with configured templates which can be used as-is in `InfluxGraph`_ configuration - see `Full Configuration Example`_ section for details. This presumes existing collection agents are using the Graphite line protocol to write to InfluxDB via its Graphite input service.

If, on the other hand, native `InfluxDB`_ metrics collection agents like `Telegraf <https://www.influxdata.com/time-series-platform/telegraf/>`_ are used, that data can too be exposed as Graphite metrics by writing appropriate template(s) in Graphite-API configuration alone.

See `Telegraf default configuration template <https://github.com/InfluxGraph/influxgraph/wiki/Telegraf-default-configuration-template>`_ for an example of this.

By default, the storage plugin makes no assumptions that data is tagged, per InfluxDB default Graphite service template configuration as below::
  
  [[graphite]]
    <..>
    # templates = []


Retention policy configuration
==============================

Pending implementation of a feature request that will allow InfluxDB to select and/or merge results from down-sampled data as appropriate, retention policy configuration is needed to support the use-case of down-sampled data being present in non default retention policies:

.. code-block:: yaml

  retention_policies:
      <time interval of query>: <retention policy name>

For example, to make a query with a group by interval of one minute or less, interval above one and less than thirty minutes and interval thirty minutes or above use the retention policies named ``default``, ``10min`` and ``30min`` respectively:

.. code-block:: yaml

  retention_policies:
      60: default
      600: 10min
      1800: 30min

While not required, retention policy interval is best kept close to or identical to ``deltas`` interval for best influx query performance.

See `Full Configuration Example`_ file for additional details.

Configuration
=======================

Minimal Configuration
----------------------

In graphite-api config file at ``/etc/graphite-api.yaml``:

.. code-block:: yaml

  finders:
    - influxgraph.InfluxDBFinder

The folowing default Graphite-API configuration is used if not provided:

.. code-block:: yaml

  influxdb:
     db: graphite

Full Configuration Example
---------------------------

See `Graphite-API example configuration file <https://github.com/InfluxGraph/influxgraph/blob/master/graphite-api.yaml.example>`_ for a complete configuration example.

Aggregation function configuration
-----------------------------------

The finder supports configurable aggregation and selector functions to use per metric path regular expression pattern. This is the equivalent of ``storage-aggregation.conf`` in Graphite's ``carbon-cache``.

Default aggregation function used is ``mean`` if no configuration provided nor any matching configuration.

InfluxGraph has pre-defined aggregation configuration matching ``carbon-cache`` defaults, namely:

.. code-block:: yaml

  aggregation_functions:
      \.min$ : min
      \.max$ : max
      \.last$ : last
      \.sum$ : sum

Defaults are overridden if ``aggregation_functions`` is configured in ``graphite-api.yaml`` as shown in configuration example.

An error will be printed to stderr if a configured aggregation function is not a known valid InfluxDB aggregation or selector method per `InfluxDB function list <https://docs.influxdata.com/influxdb/v1.1/query_language/functions/>`_.

Transformation functions, for example ``derivative``, may _not_ be used as they require a separate aggregation to be performed. Transformations are performed by Graphite-API instead, which also supports pluggable functions.

Known InfluxDB aggregation and selector functions are defined at ``influxgraph.constants.INFLUXDB_AGGREGATIONS`` and can be overriden if necessary.

.. note::

   When querying identical fields from multiple measurements InfluxDB allows only *one* aggregation function to be used for all identical fields in the query.
   
   In other words, make sure all identical InfluxDB fields matched by a Graphite query pattern, for example ``my_host.cpu.*.*`` have the same aggregation function configured.

   When using neither tagged data nor template configuration, the InfluxDB field to be queried is always ``value``. This is the case where this limitation is (most) relevant.

   ``InfluxGraph`` will use the first aggregation function configured and log a warning message to that effect if a pattern query resolves to multiple aggregation functions.


Memcached InfluxDB
------------------------

Memcached can be used to cache InfluxDB data so the `Graphite-API` can avoid querying the DB if it does not have to.

TTL configuration for memcache as shown in `Full Configuration Example`_ is only for InfluxDB series list with data query TTL set to the grouping interval used.

For example, for a query spanning twenty-four hours, a group by interval of one minute is used by default. TTL for memcache is set to one minute for that query.

For a query spanning one month, a fifteen minute group by interval is used by default. TTL is also set to fifteen minutes for that query.

Calculated intervals
--------------------

A data ``group by`` interval is automatically calculated depending on the date/time range of the query. This keeps data size in check regardless of query range and speeds up graph generation for large ranges.

Default configuration mirrors what `Grafana`_ uses with the native InfluxDB API.

Overriding the automatically calculated intervals can be done via the optional ``deltas`` configuration. See `Full Configuration Example`_ file for all supported configuration options.

Unlike other Graphite compatible data stores, InfluxDB performs aggregation on data query, not on ingestion. Queries made by InfluxGraph are therefore always aggregation queries with a group by clause.

Users that wish to retrieve all, non-aggregated, data points regardless of date/time range are advised to query `InfluxDB`_ directly.

Varnish caching InfluxDB API
----------------------------

The following is a sample configuration of `Varnish`_ as an HTTP cache in front of InfluxDB's HTTP API. It uses Varnish's default TTL of 60 sec for all InfluxDB queries.

The intention is for a local (to InfluxDB) Varnish service to cache frequently accessed data and protect the database from multiple identical requests, for example multiple users viewing the same dashboard.

InfluxGraph configuration should use Varnish port to connect to InfluxDB.

Unfortunately, given that clients like Grafana use POST requests for querying the Graphite API, which cannot be cached, using Varnish in front of a Graphite-API webapp would have no effect. Multiple requests for the same dashboard/graph will therefore still hit Graphite-API, but with Varnish in front of InfluxDB the more sensitive DB is spared from duplicated queries.

Substitute the default ``8086`` backend port with the InfluxDB API port for your installation if needed:

.. code-block:: tcl

  backend default {
    .host = "127.0.0.1";
    .port = "8086";
  }

  sub vcl_recv {
    unset req.http.cookie;
  }

Graphite API example configuration:

.. code-block:: yaml

  finders:
    - influxgraph.InfluxDBFinder
  influxdb:
    port: <varnish port>

Where ``<varnish_port>`` is Varnish's listening port.

Any other HTTP caching service will similarly work just as well.

Optional C Extensions
======================

In order of fastest to slowest, here is how the supported interpreters fare with and without C extensions. How much faster depends largely on hardware and compiler used - can expect at least `15x` and `4x` performance increases for CPython with extensions and PyPy respectively compared to standard CPython without extensions.

CPython with extensions will also use about `20x` less memory for the index than either PyPy or CPython without extensions.

#. CPython with C extensions
#. Pypy
#. CPython

There are two performance tests in the repository that can be used to see relative performance with and without extensions, for `index <https://github.com/InfluxGraph/influxgraph/blob/master/tests/index_perf.py>`_ and `template <https://github.com/InfluxGraph/influxgraph/blob/master/tests/templates_parse_perf.py>`_ functionality respectively. On PyPy extensions are purposefully disabled.

Known Limitations
===================

Data *fill* parameter and counter values
-----------------------------------------

*Changed in version 1.3.6*

As of version ``1.3.6``, the default *fill* parameter is **null** so as to not add values that do not exist in data - was ``previous`` in prior versions.

This default will break derivative calculated counter values when data sampling rate exceeds configured interval for the query - see `Calculated intervals`_.

For example, with a data sampling rate of sixty (60) seconds and default ``deltas`` configuration, queries of thirty (30) minutes and below will use a thirty (30) second interval and will contain null datapoints. This in turn causes Graphite functions like ``derivative`` and ``non_negative_derivative`` to only contain null datapoints.

The fill parameter is configurable - see `Full Configuration Example`_ - but is currently common for all metric paths.

For ``derivative`` and related functions to work, either set ``deltas`` configuration to not go below data sampling rate or set *fill* configuration to ``previous``.

Index for Graphite metric paths
--------------------------------

The index implementation via native code extension releases Python's GIL as much as possible, however, there will still be a response time increase while index is being re-built.

Without extensions response time increase will be much higher - building with extensions is highly recommended.

That said, building extensions can be disabled by running `setup.py` with the `DISABLE_INFLUXGRAPH_CEXT=1` environment variable set. A notice will be displayed by `setup.py` that extensions have been disabled.

Note that without native extension, performance is much lower and memory use of index much higher.

.. _Varnish: https://www.varnish-cache.org/
.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
