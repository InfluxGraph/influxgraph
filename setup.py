import version
from setuptools import setup, find_packages

setup(
    name='influxgraph',
    version=version.get_git_version(),
    url='https://github.com/pkittenis/influxgraph',
    license='apache2',
    author='Loosely based on original graphite-influxdb by Dieter Plaetinck, re-written from scratch by PK',
    author_email='22e889d8@opayq.com',
    description=('Influxdb storage plugin for Graphite-API'),
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
    install_requires=['graphite_api', 'influxdb>=3.0.0'],
    extras_require={
        'memcached': ['python-memcached'],
        'statsd' : ['statsd'],
    },
)
