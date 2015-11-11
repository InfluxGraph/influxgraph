Graphite-InfluxDB
=================

An `InfluxDB`_ 0.9.2 or higher plugin for `Graphite-API`_.

.. image:: https://travis-ci.org/pkittenis/graphite-influxdb.svg?branch=master
  :target: https://travis-ci.org/pkittenis/graphite-influxdb
.. image:: https://coveralls.io/repos/pkittenis/graphite-influxdb/badge.png?branch=master
  :target: https://coveralls.io/r/pkittenis/graphite-influxdb?branch=master


This project is a fork of the excellent `graphite_influxdb <https://github.com/vimeo/graphite-influxdb>`_ finder. Many thanks to Vimeo and the author for open sourcing that work.

It differs from its parent in the following ways:

* Removed Elasticsearch get series names caching integration. An HTTP cache in front of the ``graphite-api`` webapp provides better performance at significantly less overhead. See `Varnish caching InfluxDB API`_ section for an example.
* Added Memcache integration for caching of InfluxDB data
* Removed Graphite-Web support. ``graphite-influxdb`` has poor performance when used with Graphite-Web which cannot do multi fetch. Graphite-Web is not supported by this project - this is a `Graphite-API`_ only plugin.
* Simplified configuration - only InfluxDB database name for Graphite metric series is required.
* Strict flake-8 compatibility and code test coverage. This project has **100%** code test coverage.
* Python 2.6, 2.7 and 3.4 all fully supported with automated testing.

Installation
------------

::

  pip install https://github.com/pkittenis/graphite-influxdb/archive/0.5.0-rc6.tar.gz

To install with optional memcache support ::

  pip install https://github.com/pkittenis/graphite-influxdb/archive/0.5.0-rc6.tar.gz[memcache]


InfluxDB Graphite metric templates
==================================

.. note::

   Please note that InfluxDB configurations containing Graphite metric templates are currently *not* supported.
   
   Support for templates, meaning querying Graphite metrics that have been parsed into tags by InfluxDB's Graphite plugin is coming in a later version.
   
   This plugin currently requires that all Graphite metrics paths are stored as a single series.

Templates should be empty in InfluxDB's Graphite plugin configuration. ::
  
  [[graphite]]
    enabled = true
    # templates = []

Retention periods and data intervals
====================================

With InfluxDB versions >= 0.9 it is no longer required that a retention period or schema is configured explicitly for each series. Queries for series that have data in multiple retention periods are automatically merged by InfluxDB and data from all retention periods is returned.

Aggregation function configuration
==================================

The graphite-influxdb finder now supports configurable aggregation functions to use for specific metric path patterns. This is the equivalent of `storage-aggregation.conf` in Graphite's `carbon-cache`.

Default aggregation function used is `mean`, meaning `average`.

Graphite-influxdb has pre-defined aggregation configuration matching `carbon-cache` defaults, namely ::

  aggregation_functions:
      \.min$ : min
      \.max$ : max
      \.last$ : last
      \.sum$ : sum

Defaults are overridden if ``aggregation_functions`` is configured in ``graphite-api.yaml`` as shown below.

An error will be printed to stderr if a configured aggregation function is not a known valid InfluxDB aggregation method per `InfluxDB function list <https://influxdb.com/docs/v0.9/query_language/functions.html>`_.

Known InfluxDB aggregation functions are defined at ``graphite_influxdb.constants.INFLUXDB_AGGREGATIONS`` and can be overriden if necessary.

.. note::

   Please note that when querying multiple series InfluxDB allows only *one* aggregation function to be used for all series in the query.
   
   In other words, client needs to make sure all series in a wildcard query, for example ``my_host.cpu.cpu*`` have the same aggregation function configured.

   ``Graphite-InfluxDB`` `will use the first aggregation function configured <https://github.com/pkittenis/graphite-influxdb/blob/master/graphite_influxdb/classes.py#L275>`_ and log a warning message to that effect if a wildcard query resolves to multiple aggregation functions.

Schema-less design
------------------

In this project, no per series schema configuration is required, as with `InfluxDB`_.

Calculated intervals
--------------------

An interval, or step, used to group data with is automatically calculated depending on the time range of the query.

This mirrors what `Grafana`_ does when talking directly to InfluxDB.

Overriding the automatically calculated interval is not supported.

Users that wish to retrieve all data regardless of time range are advised to query `InfluxDB`_ directly.

Using with graphite-api
=======================

Please note that the version of ``graphite-api`` installed by this module's ``requirements.txt`` is at least `1.1.1` that has working multi fetch support.

Use of ``graphite-api`` version as installed by our requirements is **highly** recommended.

In your graphite-api config file at ``/etc/graphite-api.yaml``::

    finders:
      - graphite_influxdb.InfluxdbFinder
    influxdb:
       db:   graphite

The above is the most minimal configuration. There are several optional configuration options, a full list of which is below. ::

    finders:
      - graphite_influxdb.InfluxdbFinder
    influxdb:
       db:   graphite       
       host: localhost # (optional)
       port: 8086 # (optional)
       user: root # (optional)
       pass: root # (optional)
       # Log to file (optional). Default is no finder specific logging.
       log_file: /var/log/graphite_influxdb_finder/graphite_influxdb_finder.log
       # Log file logging level (optional)
       # Values are standard logging levels - info, debug, warning, critical et al
       # Default is 'info'
       log_level: info
       # (Optional) Memcache integration
       memcache:
           host: localhost
	   # TTL for /metrics/find endpoint only.
	   # TTL for /render endpoint is dynamic and based on data interval.
	   # Eg for a 24hr query which would dynamically get a 1min interval, the TTL
	   # is 1min.
	   ttl: 900 # (optional)
	   max_value: 15 # (optional) Memcache (compressed) max value length in MB.
       aggregation_functions:
           # Aggregation function for metric paths ending in 'metrics.*'
	   # is 'nonNegativeDerivative'
	   \.metrics.+$ : nonNegativeDerivative
	   # The below four aggregation functions are the
	   # defaults used if 'aggregation_functions'
	   # configuration is not provided.
	   # They will need to be re-added if configuration is provided
	   \.min$ : min
	   \.max$ : max
	   \.last$ : last
	   \.sum$ : sum

Memcache caching InfluxDB data
------------------------------

Memcache can be used to cache InfluxDB data so the `Graphite-API` webapp can avoid querying the DB if it does not have to.

TTL configuration for memcache shown above is only for `/metrics/find` endpoint with `/render` endpoint TTL being set to the data interval used.

For example, for a query spanning 24hrs, a data interval of 1min is used. TTL for memcache is set to 1min for that data.

For a query spanning 1 month, a 15min interval is used. TTL is also set to 15min for that data.

Varnish caching InfluxDB API
----------------------------

The following is a sample configuration of `Varnish`_ as an HTTP cache in front of InfluxDB's HTTP API. It uses Varnish's default TTL of 60 sec for all InfluxDB queries.

Graphite-API webapp should use Varnish port to connect to InfluxDB on each node.

Substitute the default `8086` port with the InfluxDB API port for your installation if needed  ::

  backend default {
    .host = "127.0.0.1";
    .port = "8086";
  }

  sub vcl_recv {
    unset req.http.cookie;
  }

Graphite API example configuration ::

  finders:
    - graphite_influxdb.InfluxdbFinder
  influxdb:
    db: graphite
    port: <varnish port>

Where `<varnish_port>` is Varnish's listening port.

.. _Varnish: https://www.varnish-cache.org/
.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
