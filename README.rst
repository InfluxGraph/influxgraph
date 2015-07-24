Graphite-InfluxDB
=================

An `Influxdb <https://github.com/influxdb/influxdb>`_ (0.9.2-rc1 or higher) backend for graphite-api.

.. image:: https://travis-ci.org/pkittenis/graphite-influxdb.svg?branch=master
  :target: https://travis-ci.org/pkittenis/graphite-influxdb
.. image:: https://coveralls.io/repos/pkittenis/graphite-influxdb/badge.png?branch=master
  :target: https://coveralls.io/r/pkittenis/graphite-influxdb?branch=master

This project is a fork of the excellent `graphite_influxdb <https://github.com/vimeo/graphite-influxdb>`_ finder.

It differs from its parent in the following ways:

* Removed Elasticsearch get series names caching integration. An HTTP cache in front of the ``graphite-api`` webapp provides better performance at significantly less overhead.
* Removed Graphite-Web support. ``graphite-influxdb`` has poor performance when used with Graphite-Web which cannot do multi fetch. Graphite-Web is not supported by this project - this is a Graphite-Api only finder plugin.
* Simplified configuration - only InfluxDB database name for metric series is required.
* Strict flake-8 compatibility and code test coverage. This project has *100%* code test coverage.
* Python 2.6 and 2.7 automated testing - both fully supported.
	   
Installation
-------------------

::

    pip install https://github.com/pkittenis/graphite-influxdb/releases/latest


About the retention schemas
---------------------------

In the configs below, you'll see that you need to configure the schemas (datapoint resolutions) explicitly.
It basically contains the same information as /etc/carbon/storage-schemas.conf would for whisper.
But Influxdb currently has no way to supply us this information (yet), so we must configure it explicitly here.
Also, it seems like internally, the graphite-web/graphite-api is to configure the step (resolution in seconds)
per metric (i.e. per Node/Reader), without looking at the timeframe.   I don't know how to deal with this yet (TODO), so for now it's one step per
pattern, so we don't need to specify retention timeframes.
(In fact, in the code we can assume the data exists from now to -infinity, missing data you query for
will just show up as nulls anyway)
The schema declares at which interval you should have points in InfluxDB.
Schema rules use regex and are processed in order, first match wins.  If no rule matches, 60 seconds is used.


Using with graphite-api
-----------------------

Please note that the version of ``graphite-api`` installed by this module's ``requirements.txt`` is an unreleased ``1.0.2-rc1`` that has working multi fetch support which is not in the latest official release of ``graphite-api``.

While running with the latest official release does work, performance will suffer as all series need to be fetched one-by-one.

Use ``graphite-api`` version as installed by our requirements is **highly** recommended - or latest official version >= ``1.0.2`` once ``1.0.2`` becomes available.

In your graphite-api config file at ``/etc/graphite-api.yaml``::

    finders:
      - graphite_influxdb.InfluxdbFinder
    influxdb:
       # 'db' is only required configuration setting
       db:   graphite
       host: localhost # (optional)
       port: 8086 # (optional)
       user: root # (optional)
       pass: root # (optional)
       # Log to file (optional)
       log_file: /var/log/graphite_handler/graphite_handler.log
       # Log file logging level (optional). Values are standard logging levels - info, debug, warning, critical et al
       log_level: info
       # 'schema' value is only required if schemas other than the default are needed
       schema:
         # 1 second sampling rate for metrics starting with 'high-res-metrics'
         - ['high-res-metrics', 1]
	 # 10 second sampling rate for metrics starting with 'collectd'
         - ['^collectd', 10]
	 # (optional) Default 1 min sampling rate
	 - ['', 60]
