"""InfluxGraph - Graphite-API storage finder for InfluxDB"""

from __future__ import absolute_import, print_function
from .classes.finder import InfluxDBFinder as InfluxDBFinder
from .classes.reader import InfluxDBReader as InfluxDBReader

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
