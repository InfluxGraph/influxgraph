import versioneer
from setuptools import setup, find_packages

setup(
    name='influxgraph',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url='https://github.com/pkittenis/influxgraph',
    license='apache2',
    author='PK, re-write of graphite-influxdb by Dieter Plaetinck',
    author_email='22e889d8@opayq.com',
    description=('InfluxDB storage plugin for Graphite-API'),
    long_description=open('README.rst').read(),
    packages=find_packages('.'),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Monitoring',
        ],
    install_requires=['graphite_api', 'influxdb>=3.0.0', 'python-memcached',],
    extras_require={
        'statsd' : ['statsd'],
        },
    )
