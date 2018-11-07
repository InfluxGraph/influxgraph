FROM phusion/baseimage:0.11

EXPOSE 80

ENV HOME /root
ONBUILD RUN /etc/my_init.d/00_regen_ssh_host_keys.sh
CMD ["/sbin/my_init"]

### see also brutasse/graphite-api

VOLUME /srv/graphite

RUN apt-get update && apt-get upgrade -y

# Dependencies
RUN apt-get install -y language-pack-en python-virtualenv libcairo2-dev nginx memcached python-dev libffi-dev tzdata
RUN rm -f /etc/nginx/sites-enabled/default

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
RUN locale-gen en_US.UTF-8 && dpkg-reconfigure locales

ENV TZ Etc/UTC

# add our default config and allow subsequent builds to add a different one
ADD graphite-api.yaml /etc/graphite-api.yaml
RUN chmod 0644 /etc/graphite-api.yaml
ONBUILD ADD graphite-api.yaml /etc/graphite-api.yaml
ONBUILD RUN chmod 0644 /etc/graphite-api.yaml

# Nginx service
ADD nginx.conf /etc/nginx/nginx.conf
ADD graphite_nginx.conf /etc/nginx/sites-available/graphite.conf
RUN ln -s /etc/nginx/sites-available/graphite.conf /etc/nginx/sites-enabled/
RUN mkdir /etc/service/nginx
ADD nginx.sh /etc/service/nginx/run

# Add docker host IP in hosts file on startup
ADD dockerhost.sh /etc/my_init.d/dockerhost.sh
RUN chmod +x /etc/my_init.d/dockerhost.sh

# Memcached service
RUN mkdir /etc/service/memcached
ADD memcached.sh /etc/service/memcached/run
ADD memcached.conf /etc/memcached.conf

# Install in virtualenv
RUN virtualenv /srv/graphite-env

# Activate virtualenv and add in path
ENV VIRTUAL_ENV=/srv/graphite-env
ENV PATH=/srv/graphite-env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ONBUILD ENV VIRTUAL_ENV=/srv/graphite-env
ONBUILD ENV PATH=/srv/graphite-env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Update python build tools
RUN pip install -U pip
RUN pip install -U setuptools wheel

# Install InfluxGraph, dependencies and tools for running webapp
RUN pip install -U gunicorn graphite-api influxgraph

# init scripts
RUN mkdir /etc/service/graphite-api
ADD graphite-api.sh /etc/service/graphite-api/run
RUN chmod +x /etc/service/graphite-api/run

# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
