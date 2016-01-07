import version
from setuptools import setup, find_packages
import sys

convert_2_to_3 = {}
if sys.version_info >= (3,):
    convert_2_to_3['use_2to3'] = True

setup(
    name='graphite-influxdb',
    version=version.get_git_version(),
    url='https://github.com/vimeo/graphite-influxdb',
    license='apache2',
    author='original graphite-influxdb by Dieter Plaetinck, fork by PK',
    author_email='dieter@vimeo.com and others',
    description=('Influxdb backend plugin for Graphite-API'),
    long_description=open('README.rst').read(),
    packages=find_packages('.'),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Monitoring',
    ),
    install_requires=['graphite_api', 'influxdb>=2.6.0'],
    extras_require={
        'memcached': ['python-memcached', 'gevent'],
        'statsd' : ['statsd'],
    },
    **convert_2_to_3
)
