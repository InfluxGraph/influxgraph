from .classes.finder import InfluxDBFinder as InfluxdbFinder, InfluxDBFinder
from .classes.reader import InfluxDBReader as InfluxdbReader, InfluxDBReader

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
