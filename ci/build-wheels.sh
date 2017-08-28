#!/bin/bash -xe

yum install -y libffi-devel

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install -r /io/requirements.txt
    "${PYBIN}/pip" wheel --no-deps /io/ -w wheelhouse/
done

# Bundle external shared libraries into the wheels
for whl in wheelhouse/*.whl; do
    auditwheel repair "$whl" -w /io/wheelhouse/
done

# Install packages and test
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install influxgraph --no-index -f /io/wheelhouse
    (cd "$HOME"; "${PYBIN}/python" -c 'from influxgraph.ext.nodetrie import Node; Node()')
done
