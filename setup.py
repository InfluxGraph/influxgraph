from __future__ import print_function
import platform
import os
import sys
import versioneer
from setuptools import setup, find_packages, Extension, \
     Distribution as _Distribution
from distutils.errors import CCompilerError
from distutils.errors import DistutilsExecError
from distutils.errors import DistutilsPlatformError
from distutils.command.build_ext import build_ext

cpython = platform.python_implementation() == 'CPython'

try:
    from Cython.Build import cythonize
except ImportError:
    USING_CYTHON = False
else:
    USING_CYTHON = True

ext = 'pyx' if USING_CYTHON else 'c'

extensions = [Extension("influxgraph.ext.templates",
                        ["influxgraph/ext/templates.%s" % (ext,)],
                        depends=["nodetrie/nodetrie_c/src/node.h"],
                        include_dirs=["nodetrie/nodetrie_c/src"],
                        extra_compile_args=["-std=c99", "-O3"]
                        ),
              Extension("influxgraph.ext.nodetrie",
                        ["nodetrie/nodetrie/nodetrie.c",
                         "nodetrie/nodetrie_c/src/node.c",],
                        depends=["nodetrie/nodetrie_c/src/node.h"],
                        include_dirs=["nodetrie/nodetrie_c/src"],
                        extra_compile_args=["-std=c99", "-O3"],
              ),
]

if USING_CYTHON:
    extensions = cythonize(
        extensions,
        compiler_directives={'embedsignature': True,
                             'optimize.use_switch': True,
                             'boundscheck': False,
                             'wraparound': False,
                             })

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

class BuildFailed(Exception):

    def __init__(self):
        self.cause = sys.exc_info()[1]  # work around py 2/3 different syntax

class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed()
        
cmdclass = versioneer.get_cmdclass()
cmdclass.update({'build_ext': ve_build_ext})

class Distribution(_Distribution):

    def has_ext_modules(self):
        # We want to always claim that we have ext_modules. This will be fine
        # if we don't actually have them (such as on PyPy) because nothing
        # will get built, however we don't want to provide an overally broad
        # Wheel package when building a wheel without C support. This will
        # ensure that Wheel knows to treat us as if the build output is
        # platform specific.
        return True

def run_setup(ext_modules):
    setup(
        name='influxgraph',
        version=versioneer.get_version(),
        cmdclass=cmdclass,
        url='https://github.com/InfluxGraph/influxgraph',
        license='apache2',
        author='Panos Kittenis',
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
            'Topic :: Scientific/Engineering :: Information Analysis',
            'Topic :: Scientific/Engineering :: Visualization',
            'Topic :: System :: Monitoring',
            ],
        install_requires=['influxdb>=3.0.0', 'graphite-api>=1.1.2', 'python-memcached'],
        distclass=Distribution,
        **ext_modules
        )

ext_modules = {'ext_modules': []}

if not cpython:
    run_setup(ext_modules)
    print("WARNING: C extensions are disabled on this platform,",
          "Pure Python build succeeded")
elif os.environ.get('DISABLE_INFLUXGRAPH_CEXT'):
    run_setup({'ext_modules': []})
    print("DISABLE_INFLUXGRAPH_CEXT is set - not building C extension",
          "Pure Python build succeeded")
else:
    ext_modules['ext_modules'] = extensions
    try:
        run_setup(ext_modules)
    except BuildFailed as exc:
        print(exc.cause, "WARNING: The C extension could not be compiled,",
              "building without it which will incur a performance penalty.",
              "Reasons for failure may be printed above. Build will now",
              "retry without C extension")
        run_setup({'ext_modules': []})
        print("WARNING: C extension could not be compiled.",
              "Pure Python build succeeded")
