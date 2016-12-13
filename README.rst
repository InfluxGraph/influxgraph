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


This project started as a re-write of `graphite-influxdb <https://github.com/vimeo/graphite-influxdb>`_, now a separate project.


Installation
=============

::

  pip install influxgraph

Use of a local `memcached` service is highly recommended - see configuration section on how to enable.

Mimimal configuration for Graphite-API is below. See `Full Configuration Example`_ for all possible configuration options.

``/etc/graphite-api.yaml``

::

    finders:
      - influxgraph.InfluxDBFinder

See the `Wiki <https://github.com/InfluxGraph/influxgraph/wiki>`_ and `Configuration`_ section for details.

.. contents:: Table of Contents

Docker Image
-------------

::

  docker pull ikuosu/influxgraph
  docker create  --name=influxgraph -p 8000:80 ikuosu/influxgraph
  docker start influxgraph

There will now be a Graphite-API running on ``localhost:8000`` from the container with a default InfluxDB configuration and memcache enabled. Finder expects InfluxDB to be running on ``localhost:8086`` by default.

The image will use a supplied ``graphite-api.yaml`` on build, when ``docker build`` is called on an InfluxGraph image.

Main features
==============

* InfluxDB Graphite template support - expose InfluxDB tagged data as Graphite metrics with configurable metric paths
* Dynamically calculated group by intervals based on query date/time range - fast queries regardless of the date/time they span
* Configurable per-query aggregation functions by regular expression pattern
* Configurable per-query retention policies by query date/time range. Automatically use pre-calculated downsampled data in a retention policy for historical data
* In-memory index for Graphite metric path queries
* Multi-fetch enabled - fetch data for multiple metrics with one query to InfluxDB
* Memcached integration
* Python 3 and PyPy compatibility
* Good performance even with extremely large number of metrics in the DB - generated queries are guaranteed to be have O(1) performance characteristics

Google User's Group
=====================

There is a `Google user's group for discussion <https://groups.google.com/forum/#!forum/influxgraph>`_ which is open to the public.

Goals
======

* InfluxDB as a drop-in replacement data store to the Graphite query API
* Backwards compatibility with existing Graphite API clients like Grafana and Graphite installations migrated to InfluxDB backends using Graphite input service *with or without* Graphite template configuration
* Expose native InfluxDB line protocol ingested data via the Graphite API
* Clean, readable code with complete documentation for public endpoints
* Complete code coverage with both unit and integration testing. Code has `>90%` test coverage and is integration tested against a real InfluxDB service

The two three points provide both

- A backwards compatible migration path for existing Graphite installations to use InfluxDB as a drop-in storage back-end replacement with no API client side changes required, meaning existing Grafana or other dashboards continue to work as-is.
- A way for native InfluxDB collection agents to expose their data via the *Graphite API* which allows the use of any Graphite API talking tool, the plethora of Graphite API functions, custom functions, functions across series, multi-series plotting and functions via Graphite glob expressions et al.

As of this time of writing, no alternatives exist with similar functionality and compatibility.

Non-Goals
==========

* Graphite-Web support from the official Graphite project

Dependencies
=============

With the exception of `InfluxDB`_ itself, the other dependencies are installed automatically by ``pip``.

* ``influxdb`` Python module
* `Graphite-API`_
* ``python-memcached`` Python module
* `InfluxDB`_ service

InfluxDB Graphite metric templates
==================================

`InfluxGraph` can make use of any InfluxDB data and expose them as Graphite API metrics, as well as make use of Graphite metrics added to InfluxDB as-is sans tags.

Even data written to InfluxDB by native InfluxDB API clients can be exposed as Graphite metrics, allowing transparent to clients use of the Graphite API with InfluxDB acting as its storage back-end.

To make use of tagged InfluxDB data, the finder needs to know how to generate a Graphite metric path from the tags used by InfluxDB.

The easiest way to do this is to use the Graphite plugin in InfluxDB with a configured template which can be used as-is in `InfluxGraph`_ configuration - see `Full Configuration Example`_ section for details. This presumes existing collection agents are using the Graphite line protocol to write to InfluxDB via its Graphite input service.

If, on the other hand, native `InfluxDB`_ metrics collection agents like `Telegraf <https://www.influxdata.com/time-series-platform/telegraf/>`_ are used, that data can too be exposed as Graphite metrics by writing appropriate template(s) in Graphite-API configuration alone.

See `Telegraf default configuration template <https://github.com/InfluxGraph/influxgraph/wiki/Telegraf-default-configuration-template>`_ for an example of this.

By default, the storage plugin makes no assumptions that data is tagged, per InfluxDB default Graphite service template configuration as below::
  
  [[graphite]]
    <..>
    # templates = []


Retention policy configuration
==============================

Pending implementation of a feature request that will allow InfluxDB to select and/or merge results from down-sampled data as appropriate, retention policy configuration is needed to support the use-case of down-sampled data being present in non default retention policies. ::

  retention_policies:
      <time interval of query>: <retention policy name>

For example, to make a query with a group by interval of ten minutes or less and thirty minutes or above use the retention policies named `10min` and `30min` respectively::

  retention_policies:
      600: 10min
      1800: 30min

While not required, retention policy group by interval is best kept close to or identical to ``deltas`` interval.

See `Full Configuration Example`_ file for additional details.

Configuration
=======================

Minimal Configuration
----------------------

In graphite-api config file at ``/etc/graphite-api.yaml``::

    finders:
      - influxgraph.InfluxDBFinder

The folowing default Graphite-API configuration is used if not provided::

    influxdb:
       db: graphite


Full Configuration Example
---------------------------

See `Graphite-API example configuration file <https://github.com/InfluxGraph/influxgraph/blob/master/graphite-api.yaml.example>`_ for a complete configuration example.

Aggregation function configuration
-----------------------------------

The graphite-influxdb finder supports configurable aggregation functions to use for specific metric path patterns. This is the equivalent of ``storage-aggregation.conf`` in Graphite's ``carbon-cache``.

Default aggregation function used is ``mean`` if no configuration provided nor matching.

Graphite-influxdb has pre-defined aggregation configuration matching ``carbon-cache`` defaults, namely ::

  aggregation_functions:
      \.min$ : min
      \.max$ : max
      \.last$ : last
      \.sum$ : sum

Defaults are overridden if ``aggregation_functions`` is configured in ``graphite-api.yaml`` as shown in configuration example.

An error will be printed to stderr if a configured aggregation function is not a known valid InfluxDB aggregation method per `InfluxDB function list <https://influxdb.com/docs/v0.9/query_language/functions.html>`_.

Known InfluxDB aggregation functions are defined at ``influxgraph.constants.INFLUXDB_AGGREGATIONS`` and can be overriden if necessary.

.. note::

   When querying identical fields from multiple measurements InfluxDB allows only *one* aggregation function to be used for all identical fields in the query.
   
   In other words, make sure all identical InfluxDB fields matched by a Graphite query pattern, for example ``my_host.cpu.*.*`` have the same aggregation function configured.

   When using neither tagged data nor template configuration, the InfluxDB field to be queried is always ``value``. This is the case where this limitation is (most) relevant.

   ``InfluxGraph`` will use the first aggregation function configured and log a warning message to that effect if a pattern query resolves to multiple aggregation functions.


Memcached InfluxDB
------------------------

Memcached can be used to cache InfluxDB data so the `Graphite-API` can avoid querying the DB if it does not have to.

TTL configuration for memcache as shown in `Full Configuration Example`_ is only for `/metrics/find` endpoint with `/render` endpoint TTL being set to the group by interval used.

For example, for a query spanning 24hrs, a group by interval of 1 min is used by default. TTL for memcache is set to 1 min for that data.

For a query spanning 1 month, a 15min interval is used by default. TTL is also set to 15min for that data.


Calculated intervals
--------------------

A data `group by` interval is automatically calculated depending on the date/time range of the query. This keeps data size tolerable regardless of query date/time range size and speeds up graph generation for large date/time ranges.

Default configuration mirrors what `Grafana`_ uses with the native InfluxDB API.

Overriding the automatically calculated interval is supported via the optional ``deltas`` configuration. See `Full Configuration Example`_ file for all supported configuration options.

Users that wish to retrieve all, non-aggregated, data points regardless of date/time range are advised to query `InfluxDB`_ directly.

Varnish caching InfluxDB API
----------------------------

The following is a sample configuration of `Varnish`_ as an HTTP cache in front of InfluxDB's HTTP API. It uses Varnish's default TTL of 60 sec for all InfluxDB queries.

The intention is for a local (to InfluxDB) Varnish service to cache frequently accessed data and protect the database from multiple identical requests, for example multiple users viewing the same dashboard.

Graphite-API webapp should use Varnish port to connect to InfluxDB on each node.

Unfortunately, given that clients like Grafana POST requests against the Graphite API, which cannot be cached, using Varnish in front of a Graphite-API webapp would have no effect. Multiple requests for the same dashboard/graph will therefore still hit Graphite-API webapp but with Varnish in front of InfluxDB, the more sensitive DB is spared from duplicated queries.

Substitute the default ``8086`` backend port with the InfluxDB API port for your installation if needed  ::

  backend default {
    .host = "127.0.0.1";
    .port = "8086";
  }

  sub vcl_recv {
    unset req.http.cookie;
  }

Graphite API example configuration ::

  finders:
    - influxgraph.InfluxDBFinder
  influxdb:
    port: <varnish port>

Where ``<varnish_port>`` is Varnish's listening port.

A different HTTP caching service will similarly work just as well.

Known Limitations
==================

- Index memory usage will be a factor of about 10 higher than the size of the uncompressed on disk index. For example a 100MB uncompressed on-disk index will use ~1GB of memory. This is already as low as it can be, is a hard limit imposed by Python interpreter implementation details and not likely to get any better without changes to use memory mapped file rather than loading the whole index in memory, which is AFAIK only supported on Py3 and in the index's C extension.
- On CPython interpreters, API requests while an index re-build is happening will be quite slow (a few seconds, no more than ten). PyPy does not have this problem and is recommended.

The docker image provided uses PyPy.

Contributions are most welcome to resolve any of these limitations and for anything else.

.. _Varnish: https://www.varnish-cache.org/
.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
