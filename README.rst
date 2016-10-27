InfluxGraph
=================

An `InfluxDB`_ 0.9.2 or higher storage plugin for `Graphite-API`_.

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

See the `Wiki <https://github.com/InfluxGraph/influxgraph/wiki>`_ and `Configuration`_ section for additional examples.

.. contents:: Table of Contents

Docker Image
-------------

::

  docker pull ikuosu/influxgraph
  docker create  --name=influxgraph -p 8000:80 ikuosu/influxgraph
  docker start influxgraph

There will now be a Graphite-API running on ``localhost:8000`` from the container with a default InfluxDB configuration and memcache enabled. Finder expects InfluxDB to be running on ``localhost:8086`` by default.

To use a modified Graphite-API config either build a new image from ``ikuosu/influxgraph`` with a ``graphite-api.yaml`` in the directory where ``docker build`` is called or edit the file on the container and kill gunicorn processes to make them restart.

Main features
==============

* InfluxDB Graphite template support - allows for exposure of InfluxDB tagged data as Graphite metrics
* Dynamically calculated group by intervals based on query date/time range
* Configurable per-query aggregation functions by regular expression pattern
* Configurable per-query retention policies by query date/time range. Use pre-calculated downsampled data in a retention policy for historical data automatically
* In-memory index for metric path queries
* Multi-fetch enabled - fetch data for multiple metrics with one query to InfluxDB
* Multi-query support - runs multiple queries in one statement to InfluxDB for metrics in more than one series
* Memcached integration
* Python 3 and PyPy compatibility

Goals
======

* Backwards compatibility with existing Graphite API clients like Grafana and Graphite installations migrated to InfluxDB backends using Graphite input service *with or without* Graphite template configuration
* Forwards compatibility with native InfluxDB API input data exposed via Graphite API
* Clean, readable code with complete documentation for public endpoints
* Complete code coverage with both unit and integration testing. Code has `>90%` test coverage and is integration tested against a real InfluxDB service

The two top points provide both

- A backwards compatible migration path for existing Graphite installations to use InfluxDB as a drop-in storage back-end replacement with no API client side changes required, meaning existing Grafana or other dashboards continue to work as-is.
- A forwards compatible migration path for native InfluxDB collection agents to expose their data via the *Graphite API* which allows the use of any Graphite API talking tool, the plethora of Graphite API functions, custom functions, multi-series plotting and function support et al.

As of this time of writing, no alternatives exist with similar functionality and compatibility.

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

To make use of tagged InfluxDB data, the plugin needs to know how to parse a Graphite metric path into the tags used by InfluxDB.

The easiest way to do this is to use the Graphite plugin in InfluxDB with a configured template which can be used as-is in `InfluxGraph`_ configuration, see `Full Configuration Example`_ section for details. This presumes existing metrics collection agents are using the Graphite line protocol to write to InfluxDB via its Graphite input service.

If, on the other hand, native `InfluxDB`_ metrics collection agents like `Telegraf <https://www.influxdata.com/time-series-platform/telegraf/>`_ are used, that data can too be exposed as Graphite metrics by writing appropriate template(s) in Graphite-API configuration alone.

See `Telegraf default configuration template <https://github.com/InfluxGraph/influxgraph/wiki/Telegraf-default-configuration-template>`_ for an example of this.

By default, the storage plugin makes no assumptions that data is tagged, per InfluxDB default Graphite service template configuration as below::
  
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

The folowing default Graphite-API configuration is used if not provided::

    influxdb:
       db: graphite


Full Configuration Example
---------------------------

::

    finders:
      - influxgraph.InfluxDBFinder
    influxdb:
        ## InfluxDB configuration
	# 
        db: graphite
        host: localhost # (optional)
        port: 8086 # (optional)
        user: root # (optional)
        pass: root # (optional)
	
	## Logging configuration
	# 
        # Log to file (optional). Default is no finder specific logging.
        log_file: /var/log/influxgraph/influxgraph_finder.log
        # Log file logging level (optional)
        # Values are standard logging levels - `info`, `debug`, `warning`, `critical` et al
        # Default is `info`
        log_level: info
	
	## Graphite Template Configuration
	# 
	# (Optional) Graphite template configuration
	# One template per line, identical to InfluxDB Graphite input service template configuration
	# See https://github.com/influxdata/influxdb/tree/master/services/graphite for template
	# configuration documentation.
	# 
	# Note that care should be taken so that InfluxDB template configuration
	# results in sane measurement and field names that do not override each other.
	# 
	# InfluxGraph will run multiple queries in the same statement where multiple
	# tag values are requested for the same measurement and/or field.
	# 
	# For best InfluxDB performance and so that data can be queried correctly 
	# by InfluxGraph, fewer measurements with multiple fields are preferred.
	# 
	# NB - separator for templates is not configurable as of yet
	# 
	templates:
	  # 
	  # Template format: [filter] <template> [tag1=value1,tag2=value2]
	  # 
	  ##  Filter, template and extra static tags
	  # 
	  # For a metric path `production.my_host.cpu.cpu0.load` the following template will
	  # filter on metrics starting with `production`,
          # use tags `environment`, `host` and `resource` with measurement name `cpu0.load`
	  # and extra static tags `region` and `agent` set to `us-east-1` and
	  # `sensu` respectively
          - production.* environment.host.resource.measurement* region=us-east1,agent=sensu
	  
	  # 
	  ## Template only
	  # The following template does not use filter or extra tags.
          # For a metric path `my_host.cpu.cpu0.load` it will use tags `host` and `resource` 
	  # with measurement name `cpu0.load`
	  - host.resource.measurement*
	  
	  # 
	  ## Drop prefix, template with tags after measurement
	  # For a metric path `stats.load.my_host.cpu` the following template will use tags
	  # `host` and `resource` and remove `stats` prefix from metric paths
	  - stats.* ..measurement.host.resource
	  
	  #
	  ## Measurement with multiple fields
	  # For metric paths `my_host.cpu-0.cpu-idle`, `my_host.cpu-0.cpu-user` et al, the
	  # following template will use tag `host` with measurement name `cpu-0` and fields
	  # `cpu-idle`, `cpu-user` et al
	  - host.measurement.field*
	  
	  # NB - A catch-all template of `measurement*` _should not_ be used - 
	  # that is the default and would have the same effect as if no template was provided
	  # 
	  ## Examples from InfluxDB Graphite service configuration
	  # 
          ## filter + template
	  # - *.app env.service.resource.measurement
	  
	  ## filter + template + extra tag
	  # - stats.* .host.measurement* region=us-west,agent=sensu
	  
	  # filter + template with field key
	  # - stats.* .host.measurement.field*
	
        ## (Optional) Memcache integration
	# 
        memcache:
          host: localhost
	  # TTL for /metrics/find endpoint only.    
	  # TTL for /render endpoint is dynamic and based on data interval.    
	  # Eg for a 24hr query which would dynamically get a 1min interval, the TTL    
	  # is 1min.    
	  ttl: 900 # (optional)    
	  max_value: 1 # (optional) Memcache (compressed) max value length in MB.    
	
	## (Optional) Aggregation function configuration
	# 
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
	  
	  ## Query Retention Policy configuration
	  # 
 	  # (Optional) Retention policies to use for associated time intervals.
 	  # Key is query time interval in seconds, value the retention policy name a
	  # query with the associated time interval, or above, should use.
	  # 
	  # For best performance, retention policies should closely match time interval
	  # (delta) configuration values. For example, where delta configuration sets
	  # queries 28days and below to use 15min intervals, retention policies would
	  # have configuration to use an appropriate retention policy for queries with
	  # 15min or above intervals.
	  # 
	  # That said, there is no requirement that the settings be the same.
	  # 
	  # Eg to use a retention policy called `30m` policy for intervals
	  # of thirty minutes and above, `10m` for queries with a time
	  # interval between thirty to ten minutes and `default` for intervals
	  # between ten to five minutes:
          retention_policies:
	    1800: 30m
	    600: 10m
	    300: default


Aggregation function configuration
-----------------------------------

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

Known InfluxDB aggregation functions are defined at ``influxgraph.constants.INFLUXDB_AGGREGATIONS`` and can be overriden if necessary.

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

A data `group by` interval is automatically calculated depending on the date/time range of the query. This keeps data size tolerable regardless of query date/time range size and speeds up graph generation for large date/time ranges.

Default configuration mirrors what `Grafana`_ uses when talking directly to InfluxDB.

Overriding the automatically calculated interval is supported via the optional ``deltas`` configuration. See `Full Configuration Example`_ section for all supported configuration options.

Users that wish to retrieve all data points regardless of date/time range are advised to query `InfluxDB`_ directly.


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
    db: graphite
    port: <varnish port>

Where ``<varnish_port>`` is Varnish's listening port.

A different HTTP caching service will similarly work just as well.

Known Limitations
==================

- In memory index can use *a lot* of memory in InfluxDB installations with a large number of unique metrics (> 1M). `Pypy <http://pypy.org>`_ is recommended in that case which allows for a much lower memory footprint compared to the CPython intepreter.

The docker container in this document uses PyPy.


.. _Varnish: https://www.varnish-cache.org/
.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
