Graphite-InfluxDB
=================

An `InfluxDB`_ 0.9.2 or higher plugin for `Graphite-API`_.

.. image:: https://travis-ci.org/pkittenis/graphite-influxdb.svg?branch=master
  :target: https://travis-ci.org/pkittenis/graphite-influxdb
.. image:: https://coveralls.io/repos/pkittenis/graphite-influxdb/badge.png?branch=master
  :target: https://coveralls.io/r/pkittenis/graphite-influxdb?branch=master


This project is a fork of the excellent `graphite_influxdb <https://github.com/vimeo/graphite-influxdb>`_ finder.

It differs from its parent in the following ways:

* Removed Elasticsearch get series names caching integration. An HTTP cache in front of the ``graphite-api`` webapp provides better performance at significantly less overhead.
* Removed Graphite-Web support. ``graphite-influxdb`` has poor performance when used with Graphite-Web which cannot do multi fetch. Graphite-Web is not supported by this project - this is a Graphite-Api only finder plugin.
* Simplified configuration - only InfluxDB database name for Graphite metric series is required.
* Strict flake-8 compatibility and code test coverage. This project has **100%** code test coverage.
* Python 2.6, 2.7 and 3.4 all fully supported with automated testing.

Installation
------------

::

    pip install https://github.com/pkittenis/graphite-influxdb/archive/0.5.0-rc3.tar.gz


Retention periods and data intervals
====================================

With InfluxDB versions >= 0.9 it is no longer required that a retention period or schema is configured explicitly for each series. Queries for series that have data in multiple retention periods are automatically merged by InfluxDB and data from all retention periods is returned.

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

Please note that the version of ``graphite-api`` installed by this module's ``requirements.txt`` is an unreleased ``1.0.2-rc1`` that has working multi fetch support which is not in the latest official release of ``graphite-api``.

While running with the latest official release does work, performance will suffer as multiple series will need to be retrieved one-by-one.

Use of ``graphite-api`` version as installed by our requirements is **highly** recommended - or latest official version >= ``1.0.2`` once ``1.0.2`` becomes available.

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

.. _Graphite-API: https://github.com/brutasse/graphite-api
.. _Grafana: https://github.com/grafana/grafana
.. _InfluxDB: https://github.com/influxdb/influxdb
