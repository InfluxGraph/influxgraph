InfluxGraph
=================

An `InfluxDB`_ 0.9.2 or higher storage plugin for `Graphite-API`_.

.. image:: https://travis-ci.org/pkittenis/influxgraph.svg?branch=master
  :target: https://travis-ci.org/pkittenis/influxgraph
.. image:: https://coveralls.io/repos/pkittenis/influxgraph/badge.png?branch=master
  :target: https://coveralls.io/r/pkittenis/influxgraph?branch=master


This project started as a re-write of `graphite_influxdb <https://github.com/vimeo/graphite-influxdb>`_ - many thanks to Vimeo and the author for open sourcing that work.

Main features

* Dynamically calculated group by intervals based on query date range to speed up graph generation for large date ranges
* Configurable per-query aggregation functions by pattern
* In-memory index for metric path queries
* Multi-fetch enabled - fetch data for multiple series with one query to InfluxDB
* Memcached integration

Installation
------------

::

  pip install influxgraph

Use of a local `memcached` service is highly recommended - see configuration section on how to enable.

Mimimal configuration for Graphite-API is below. See `Full Configuration Example`_ for all possible configuration options.

``/etc/graphite-api.yaml``

::

    finders:
      - influxgraph.InfluxDBFinder
    influxdb:
       db: graphite

Dependencies
-------------

 * ``influxdb`` Python module
 * `Graphite-API`_
 * ``python-memcached`` Python module
 * `InfluxDB`_

InfluxDB Graphite metric templates
==================================

.. note::

   Please note that InfluxDB configurations containing Graphite metric templates are currently *not* supported.
   
   Support for templates, meaning querying Graphite metrics that have been parsed into tags by InfluxDB's Graphite plugin is not yet possible.
   
   This plugin currently requires that all Graphite metrics paths are stored as a single measurement.
   
   Contributions to fully support templated Graphite data are most welcome.

Templates should be empty in InfluxDB's Graphite plugin configuration. ::
  
  [[graphite]]
    enabled = true
    # templates = []


Retention policy configuration
==============================

Pending implementation of a feature request that will allow InfluxDB to select and/or merge results from multiple retention policies as appropriate, retention policy configuration is needed to support the use-case of down-sampled data being present in non default retention policies. ::

  retention_policies:
      <time interval of query>: <retention policy name>

For example, to make a query with a time interval of ten and thirty minutes use the retention policies named `10min` and `30min` respectively::

  retention_policies:
      600: 10min
      1800: 30min

While not required, retention policy time interval is best kept close to or identical to ``deltas`` interval.

Configuration
=======================

Minimal Configuration
----------------------

In graphite-api config file at ``/etc/graphite-api.yaml``::

    finders:
      - influxgraph.InfluxDBFinder
    influxdb:
       db: graphite

Full Configuration Example
---------------------------

::

    finders:
      - influxgraph.InfluxDBFinder
    influxdb:
       db: graphite       
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
	   max_value: 1 # (optional) Memcache (compressed) max value length in MB.
       aggregation_functions:
	   # The below four aggregation functions are the
	   # defaults used if 'aggregation_functions'
	   # configuration is not provided.
	   # They will need to be re-added if configuration is provided
	   \.min$ : min
	   \.max$ : max
	   \.last$ : last
	   \.sum$ : sum
	# (Optional) Time intervals to use for query time ranges
	# Key is time range of query, value is time delta of query.
	# Eg to use a one second query interval for a query spanning
	# one hour or less use `3600 : 1`
	# Shown below is the default configuration, change/add/remove
	# as necessary.
        deltas:
            # 1 hour -> 1s
            # 3600 : 1
            # 1 day -> 30s
            # 86400 : 30
            # 3 days -> 1min
            259200 : 60
            # 7 days -> 5min
            604800 : 300
            # 14 days -> 10min
            1209600 : 600
            # 28 days -> 15min
            2419200 : 900
            # 2 months -> 30min
            4838400 : 1800
            # 4 months -> 1hour
            9676800 : 3600
            # 12 months -> 3hours
            31536000 : 7200
            # 4 years -> 12hours
            126144000 : 43200
	# (Optional) Retention policies to use for associated time intervals.
	# Key is time interval in seconds, value the retention policy name a
	# query with the associated time interval or less should use.
	# 
	# For best performance, retention policies should closely match time interval
	# (delta) configuration values. For example, where delta configuration sets
	# queries 28days and below to use 15min intervals, retention policies would
	# have configuration to use an appropriate retention policy for queries with
	# 15min or above intervals.
	# 
	# That said, there is no requirement that the settings be the same.
	# 
	# Eg to use retention policy called `30m` policy for intervals
	# of thirty minutes and above, `10m` for queries with a time
	# interval of ten minutes and above and `default` for intervals 
	# five minutes and below:
        retention_policies:
	    1800: 30m
	    600: 10m
	    300: default

Aggregation function configuration
==================================

The graphite-influxdb finder now supports configurable aggregation functions to use for specific metric path patterns. This is the equivalent of ``storage-aggregation.conf`` in Graphite's ``carbon-cache``.

Default aggregation function used is ``mean``, meaning ``average``.

Graphite-influxdb has pre-defined aggregation configuration matching ``carbon-cache`` defaults, namely ::

  aggregation_functions:
      \.min$ : min
      \.max$ : max
      \.last$ : last
      \.sum$ : sum

Defaults are overridden if ``aggregation_functions`` is configured in ``graphite-api.yaml`` as shown in configuration section.

An error will be printed to stderr if a configured aggregation function is not a known valid InfluxDB aggregation method per `InfluxDB function list <https://influxdb.com/docs/v0.9/query_language/functions.html>`_.

Known InfluxDB aggregation functions are defined at ``graphite_influxdb.constants.INFLUXDB_AGGREGATIONS`` and can be overriden if necessary.

.. note::

   Please note that when querying multiple series InfluxDB allows only *one* aggregation function to be used for all series in the query.
   
   In other words, client needs to make sure all series in a wildcard query, for example ``my_host.cpu.cpu*`` have the same aggregation function configured.

   ``InfluxGraph`` will use the first aggregation function configured and log a warning message to that effect if a wildcard query resolves to multiple aggregation functions.

Memcache caching InfluxDB data
------------------------------

Memcache can be used to cache InfluxDB data so the `Graphite-API` webapp can avoid querying the DB if it does not have to.

TTL configuration for memcache shown above is only for `/metrics/find` endpoint with `/render` endpoint TTL being set to the data interval used.

For example, for a query spanning 24hrs, a data interval of 1 min is used by default. TTL for memcache is set to 1 min for that data.

For a query spanning 1 month, a 15min interval is used. TTL is also set to 15min for that data.


Calculated intervals
--------------------

A data `group by` interval is automatically calculated depending on the time range of the query.

This mirrors what `Grafana`_ does when talking directly to InfluxDB.

Overriding the automatically calculated interval is supported via the optional ``deltas`` configuration. See `Full Configuration Example`_ section for all supported configuration options.

Users that wish to retrieve all data regardless of time range are advised to query `InfluxDB`_ directly.


Varnish caching InfluxDB API
----------------------------

The following is a sample configuration of `Varnish`_ as an HTTP cache in front of InfluxDB's HTTP API. It uses Varnish's default TTL of 60 sec for all InfluxDB queries.

The intention is for a local (to InfluxDB) Varnish to cache frequently accessed data and protect the database from multiple identical requests, for example multiple users viewing the same dashboard.

Graphite-API webapp should use Varnish port to connect to InfluxDB on each node.

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
    - graphite_influxdb.InfluxdbFinder
  influxdb:
    db: graphite
    port: <varnish port>

Where ``<varnish_port>`` is Varnish's listening port.

.. _Varnish: https://www.varnish-cache.org/
.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
